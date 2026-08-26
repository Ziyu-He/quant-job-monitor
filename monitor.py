#!/usr/bin/env python3
"""
Job Monitor — runs daily via launchd, checks career pages + LinkedIn for new postings.
Notifies via macOS system notification + email when relevant new roles appear.
"""

import os, json, hashlib, sqlite3, subprocess, logging, shutil, tempfile
import urllib.parse, time, re
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from Crypto.Cipher import AES
from openai import OpenAI

# ─── PATHS ────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
STATE_FILE = BASE_DIR / "state" / "snapshots.json"
PENDING_FILE = BASE_DIR / "state" / "pending.json"
LOG_FILE   = BASE_DIR / "monitor.log"

# ─── CONFIG ───────────────────────────────────────────────────────────────────
def load_env():
    env_file = BASE_DIR / ".env"
    if not env_file.exists():
        return
    # BASE_DIR lives under an iCloud-synced Desktop; a sync lock at read time can
    # raise OSError (EDEADLK "Resource deadlock avoided") and crash startup.
    # Retry a few times so a transient lock doesn't skip the whole daily run.
    text = None
    for attempt in range(5):
        try:
            text = env_file.read_text()
            break
        except OSError as e:
            if attempt == 4:
                raise
            time.sleep(2)
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

load_env()

DEEPSEEK_KEY    = os.getenv("DEEPSEEK_API_KEY", "")
RESEND_API_KEY  = os.getenv("RESEND_API_KEY", "")
EMAIL_TO        = "ziyuhe030524@gmail.com"

NOTION_DB_ID    = "447e2e097a1244f682c0e61d339f74a5"
NOTION_COL_ID   = "34a63abc-da67-42ce-b945-dbddc2dd7a0e"
NOTION_SPACE_ID = "681ea971-7118-818b-ac8f-000316d0ddb0"
NOTION_VIEW_ID  = "341f0ae9-2de9-4bb7-843d-19987f1f578a"

ALERT_EXPIRY_DAYS = 7   # reset "Confirmed Open" status after this many days

# LinkedIn job searches — posted within last 7 days
# Narrowed to a few ORTHOGONAL broad terms (2026-08-13). The old 14 narrow
# phrases ("quant researcher new grad", "quant trader 2026", …) were highly
# redundant subsets: LinkedIn keyword search on the broad word "quant" already
# returns quant researcher/trader/developer/analyst roles, so splitting them just
# re-searched the same jobs (3× the requests → rate-limited + slow, ~5h/run with
# no extra jobs). Broad terms + deep pagination + the LLM/experience filter give
# the same recall at a fraction of the request/eval volume.
#   • "quantitative" / "quant" — spelled-out and shorthand (LinkedIn matches by
#     token, so "quant" does not reliably hit "quantitative"; keep both).
#   • "trader" / "trading" — trading-desk & trading-systems roles.
#   • "index" — index analyst/researcher/developer.
#   • "portfolio management" — PM roles.
LINKEDIN_KEYWORDS = [
    "quantitative",
    "quant",
    "trader",
    "trading",
    "index",
    "portfolio management",
]

LINKEDIN_LOCATIONS = [
    "United States",
    "United Kingdom",
    "Hong Kong",
    "Singapore",
    "Netherlands",
]

USER_PROFILE = """
CMU MSCF (Computational Finance) student, graduating December 2026 — entry-level candidate
open to junior roles whose minimum requirement is under 3 years (0-2 years).
International student — requires visa sponsorship (H-1B / OPT).
Target roles (new-grad / entry-level through roles requiring at most 3 years of experience):
              Quantitative Researcher, Quantitative Trader, Quantitative Developer,
              Quantitative Analyst, Trader / Trading (quant or systematic),
              Index Analyst, Index Researcher, Portfolio Manager / Portfolio Management.
              (ONLY these quant / trading / index / portfolio functions — NOT software engineering.)
Preferred locations: US (NYC, Chicago, SF, Boston, Stamford CT), London, Hong Kong,
  Singapore, Netherlands. Remote-only also ok.
NOT relevant:
  - Internships
  - Any role whose MINIMUM required experience is 3 or more years (e.g. "3+ years",
    "minimum 3 years", "at least 3 years", "3-5 years", "5+ years").
    Roles whose minimum is 0-2 years (incl. "1-3 years", "2-4 years", "up to 3 years",
    new-grad, or unspecified) ARE acceptable.
  - Senior management / leadership titles, e.g. anything containing:
    "Director", "VP", "Vice President", "Head of", "Principal",
    "Chief", "C-suite", "President", "Executive", "Managing Director", "Partner"
    (note: "Senior"/"Lead"/"Manager" alone is acceptable if experience ≤3 years)
  - Non-quant roles (sales, marketing, HR, general ops, etc.)
  - ANY software-engineering role — the candidate does NOT want software engineering:
    "Software Engineer", "Software Developer", Java / Python / C++ / C# / full-stack /
    backend / frontend / web / mobile / DevOps / platform engineer or developer — EVEN if
    described as "for quant/trading systems", low-latency, or at a quant/trading firm.
    (BUT "Quantitative Developer" / "Quant Developer" IS wanted — that is a quant role,
    not a software-engineering role.)
  - Locations outside US, UK/London, Hong Kong, Singapore, Netherlands (incl. Australia/New Zealand — no longer targeted)
"""

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

# On GitHub Actions the file log isn't visible in the run console (and isn't
# committed back if the run fails), leaving the cloud run a black box. Also
# stream logs to stdout there so progress/throttling are visible. Local runs
# (launchd) are unaffected — this only triggers under Actions / static mode.
if os.getenv("GITHUB_ACTIONS") == "true" or os.getenv("USE_STATIC_COMPANIES") == "1":
    _console = logging.StreamHandler()
    _console.setFormatter(logging.Formatter("%(asctime)s  %(levelname)s  %(message)s"))
    logging.getLogger().addHandler(_console)


def wait_for_network(max_wait: int = 3600, interval: int = 15) -> bool:
    """Block until a TCP connection to 8.8.8.8:53 succeeds or max_wait seconds elapse."""
    import socket
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect(("8.8.8.8", 53))
            s.close()
            return True
        except OSError:
            log.info("Network not ready, retrying in %ds…", interval)
            time.sleep(interval)
    return False


# ─── NOTION: auto-fetch token from desktop app ────────────────────────────────
def get_notion_token() -> str | None:
    cookies_src = Path.home() / "Library/Application Support/Notion/Partitions/notion/Cookies"
    if not cookies_src.exists():
        log.error("Notion desktop app cookies not found")
        return None
    tmp = Path(tempfile.mktemp(suffix=".db"))
    shutil.copy(cookies_src, tmp)
    try:
        cur = sqlite3.connect(str(tmp)).cursor()
        cur.execute("SELECT encrypted_value FROM cookies WHERE name='token_v2'")
        row = cur.fetchone()
        if not row:
            return None
        enc_val = row[0]
        pw = subprocess.run(
            ["security", "find-generic-password", "-s", "Notion Safe Storage", "-w"],
            capture_output=True, text=True,
        ).stdout.strip()
        if not pw:
            return None
        import hashlib as hl
        key = hl.pbkdf2_hmac("sha1", pw.encode(), b"saltysalt", 1003, dklen=16)
        raw = AES.new(key, AES.MODE_CBC, b" " * 16).decrypt(enc_val[3:])
        raw = raw[: -raw[-1]].decode("latin-1")
        idx = raw.find("v03")
        return urllib.parse.unquote(raw[idx:]) if idx >= 0 else None
    finally:
        tmp.unlink(missing_ok=True)


# ─── NOTION: get schema (property name → internal ID) ────────────────────────
def get_schema(token: str) -> dict:
    headers = {"Cookie": f"token_v2={token}", "Content-Type": "application/json"}
    try:
        resp = requests.post(
            "https://www.notion.so/api/v3/syncRecordValues",
            headers=headers,
            json={"requests": [{"pointer": {"table": "collection", "id": NOTION_COL_ID}, "version": -1}]},
            timeout=15,
        )
    except Exception as e:
        log.error(f"get_schema network error: {e}")
        return {}
    if resp.status_code != 200:
        return {}
    schema = resp.json().get("recordMap", {}).get("collection", {}).get(NOTION_COL_ID, {}).get("value", {}).get("value", {}).get("schema", {})
    return {v.get("name"): k for k, v in schema.items()}


# Companies whose Notion "Careers Link" points at a marketing LANDING page (no
# real job list) instead of the actual job board. The monitor scrapes the
# landing page and sees only prose → silently misses every real posting. Until
# the Notion link itself is corrected, this map redirects the fetch to the real
# job board (found manually). Keyed by exact company name.
URL_OVERRIDES = {
    # Squarepoint's /early-careers is a marketing page; real board is Greenhouse.
    "Squarepoint Capital": "https://boards-api.greenhouse.io/v1/boards/squarepointcapital/jobs",
    # WorldQuant's career-listing page is JS-rendered; real board is Greenhouse (101 jobs).
    "WorldQuant": "https://boards-api.greenhouse.io/v1/boards/worldquant/jobs",
    # Quantbot's /careers embeds a Greenhouse board (slug quantbot-technologies).
    "Quantbot": "https://boards-api.greenhouse.io/v1/boards/quantbot-technologies/jobs",
    # Trexquant's /careers is a JS SPA proxying Workable (slug trexquant).
    "Trexquant": "https://apply.workable.com/api/v1/widget/accounts/trexquant",
    # Flow Traders' /careers/graduates is a marketing page; real board is Greenhouse (45 jobs,
    # incl. Graduate Quant Trader). Was silently missing every Flow Traders opening.
    "Flow Traders": "https://boards-api.greenhouse.io/v1/boards/flowtraders/jobs",
    # Graham Capital's /careers is a marketing page; real board is Greenhouse (slug
    # grahamcapitalmanagement, 10 jobs incl. Quantitative Research Analyst). Was missing all.
    "Graham Capital Management": "https://boards-api.greenhouse.io/v1/boards/grahamcapitalmanagement/jobs",
    # ── 2026-08-18 full-list landing-page audit: 16 more companies whose monitored URL
    #    was a marketing page hiding a real board (verified each yields jobs). ──
    "Akuna Capital": "https://boards-api.greenhouse.io/v1/boards/akunacapital/jobs",
    "Cross River": "https://boards-api.greenhouse.io/v1/boards/crossriverbank/jobs",
    "EverQuote": "https://boards-api.greenhouse.io/v1/boards/everquote/jobs",
    "Five Rings": "https://boards-api.greenhouse.io/v1/boards/fiveringsllc/jobs",
    "PDT Partners": "https://boards-api.greenhouse.io/v1/boards/pdtpartners/jobs",
    "Perpay": "https://boards-api.greenhouse.io/v1/boards/perpay/jobs",
    "Point72 / Cubist Systematic": "https://boards-api.greenhouse.io/v1/boards/point72/jobs",
    "Schonfeld": "https://boards-api.greenhouse.io/v1/boards/schonfeld/jobs",
    "Summit": "https://boards-api.greenhouse.io/v1/boards/summitpartnerslp/jobs",
    "TransMarket Group": "https://boards-api.greenhouse.io/v1/boards/transmarketgroup/jobs",
    "BHG Financial": "https://jobs.lever.co/bhg-inc",
    "Belvedere Trading": "https://jobs.lever.co/belvederetrading",
    "Syntax": "https://jobs.lever.co/syntax",
    "Wintermute": "https://jobs.lever.co/wintermute-trading",
    "ADIA": "https://apply.workable.com/api/v1/widget/accounts/adia",
    "Caxton": "https://apply.workable.com/api/v1/widget/accounts/caxton",
    # ── 2026-08-18 audit, Workday landing pages (26, each verified to yield jobs). ──
    "PIMCO": "https://pimco.wd1.myworkdayjobs.com/wday/cxs/pimco/pimco-careers/jobs",
    "TD Bank": "https://td.wd3.myworkdayjobs.com/wday/cxs/td/TD_Bank_Careers/jobs",
    "Robeco": "https://robeco.wd3.myworkdayjobs.com/wday/cxs/robeco/robecoexternalcareers/jobs",
    "S&P Global": "https://spgi.wd5.myworkdayjobs.com/wday/cxs/spgi/SPGI_Careers/jobs",
    "S&P500": "https://spgi.wd5.myworkdayjobs.com/wday/cxs/spgi/SPGI_Careers/jobs",
    "T. Rowe Price": "https://troweprice.wd5.myworkdayjobs.com/wday/cxs/troweprice/TRowePrice/jobs",
    "Voya": "https://godirect.wd5.myworkdayjobs.com/wday/cxs/godirect/voya_jobs/jobs",
    "Wellington Management": "https://wellington.wd5.myworkdayjobs.com/wday/cxs/wellington/External/jobs",
    "Invesco": "https://invesco.wd1.myworkdayjobs.com/wday/cxs/invesco/IVZearlycareers/jobs",
    "Federal Home Loan Bank of New York": "https://fhlbny.wd1.myworkdayjobs.com/wday/cxs/fhlbny/FHLBNY/jobs",
    "LSEG": "https://lseg.wd3.myworkdayjobs.com/wday/cxs/lseg/Careers/jobs",
    "M&G Investments": "https://mgpru.wd3.myworkdayjobs.com/wday/cxs/mgpru/mandgprudential/jobs",
    "MFS": "https://mfs.wd1.myworkdayjobs.com/wday/cxs/mfs/MFS-Careers/jobs",
    "Lombard Odier": "https://lombardodier.wd3.myworkdayjobs.com/wday/cxs/lombardodier/Lombard_Odier_Careers/jobs",
    "Dodge & Cox": "https://dodgeandcox.wd5.myworkdayjobs.com/wday/cxs/dodgeandcox/Dodgecox/jobs",
    "Capital Research": "https://capgroup.wd1.myworkdayjobs.com/wday/cxs/capgroup/capitalgroupcareers/jobs",
    "DWS": "https://db.wd3.myworkdayjobs.com/wday/cxs/db/DWSWebsite/jobs",
    "Apollo": "https://athene.wd5.myworkdayjobs.com/wday/cxs/athene/Apollo_Careers/jobs",
    "Eastspring Investments": "https://prudential.wd3.myworkdayjobs.com/wday/cxs/prudential/prudential_eastspring/jobs",
    "AllianceBernstein": "https://abglobal.wd1.myworkdayjobs.com/wday/cxs/abglobal/alliancebernsteincareers/jobs",
    "Dimensional": "https://dimensional.wd5.myworkdayjobs.com/wday/cxs/dimensional/DFA_Careers/jobs",
    "American Bankers Association": "https://aba.wd1.myworkdayjobs.com/wday/cxs/aba/ABA/jobs",
    "Brookfield": "https://brookfield.wd5.myworkdayjobs.com/wday/cxs/brookfield/brookfield/jobs",
    "Centrica Energy": "https://centrica.wd3.myworkdayjobs.com/wday/cxs/centrica/Centrica/jobs",
    "Capital One (Data Science)": "https://capitalone.wd12.myworkdayjobs.com/wday/cxs/capitalone/Capital_One/jobs",
    "Capital One (Quant)": "https://capitalone.wd12.myworkdayjobs.com/wday/cxs/capitalone/Capital_One/jobs",
    # G-Research careers URL was the marketing homepage (gresearch.com) → 0 jobs.
    # Real board is Workday; store the CXS API form so fetch_workday handles it
    # (full postings + per-job links + dates), not the human SPA page (page-1 only).
    "G-research": "https://gresearch.wd103.myworkdayjobs.com/wday/cxs/gresearch/G-Research/jobs",
    # Marshall Wace launched a real Greenhouse grad board (mw-tech-grad) in
    # Aug 2026 — Quant Research/Quant Developer 2027. The old careers URL
    # (mwam.com/join-us/quantitative) is marketing only; apply link lives on
    # the programme page → Greenhouse. Point straight at the board API.
    "Marshall Wace": "https://boards-api.greenhouse.io/v1/boards/mw-tech-grad/jobs?content=true",
    # Millennium careers URL was mlp.com/careers/students (marketing) → 0 jobs.
    # Real board is Eightfold; its pcsx/search 403s so use apply/v2/jobs (224 jobs).
    "Millennium Management": "https://mlp.eightfold.ai/api/apply/v2/jobs?domain=mlp.com",
    # Capula careers page is marketing; real board is Workable.
    "Capula": "https://apply.workable.com/api/v1/widget/accounts/capula-investment-management-ltd",
}


# ─── NOTION: query all companies ─────────────────────────────────────────────
def get_companies(token: str, prop_ids: dict) -> list[dict]:
    headers = {"Cookie": f"token_v2={token}", "Content-Type": "application/json"}

    careers_id = prop_ids.get("Careers Link", ";L:F")
    title_id   = "title"

    block_ids, cursor = [], None
    while True:
        loader = {
            "type": "reducer",
            "reducers": {"collection_group_results": {"type": "results", "limit": 1000}},
            "searchQuery": "",
            "userTimeZone": "America/New_York",
        }
        if cursor:
            loader["reducers"]["collection_group_results"]["cursor"] = cursor

        body = {
            "collection":     {"id": NOTION_COL_ID,  "spaceId": NOTION_SPACE_ID},
            "collectionView": {"id": NOTION_VIEW_ID, "spaceId": NOTION_SPACE_ID},
            "query2": {},
            "loader": loader,
        }
        resp = requests.post(
            "https://www.notion.so/api/v3/queryCollection",
            headers=headers, json=body, timeout=30,
        )
        if resp.status_code != 200:
            log.error(f"queryCollection failed: {resp.status_code} {resp.text[:200]}")
            break

        data    = resp.json()
        results = data.get("result", {}).get("reducerResults", {}).get("collection_group_results", {})
        ids     = results.get("blockIds", [])
        block_ids.extend(ids)

        if not results.get("hasMore", False) or not ids:
            break
        cursor = {"stack": [[{"table": "block", "id": ids[-1], "index": len(block_ids)}]]}

    companies, skipped_no_link = [], 0
    for i in range(0, len(block_ids), 100):
        batch = block_ids[i:i + 100]
        resp = requests.post(
            "https://www.notion.so/api/v3/syncRecordValues",
            headers=headers,
            json={"requests": [{"pointer": {"table": "block", "id": bid}, "version": -1} for bid in batch]},
            timeout=30,
        )
        if resp.status_code != 200:
            log.error(f"syncRecordValues failed: {resp.status_code} {resp.text[:200]}")
            continue

        blocks = resp.json().get("recordMap", {}).get("block", {})
        for bid in batch:
            props = blocks.get(bid, {}).get("value", {}).get("value", {}).get("properties", {})
            if not props:
                continue
            title_arr = props.get(title_id, [])
            name = "".join(t[0] for t in title_arr if isinstance(t, list) and t and isinstance(t[0], str))
            url_arr = props.get(careers_id, [])
            url = url_arr[0][0] if url_arr and url_arr[0] else ""
            url = URL_OVERRIDES.get(name, url)   # redirect known landing-page misconfigs
            if name and url and url.startswith("http"):
                companies.append({"id": bid, "name": name, "url": url})
            elif name:
                skipped_no_link += 1

    if skipped_no_link:
        log.info(f"Skipped {skipped_no_link} companies with no Careers Link filled in")

    return companies


def load_companies() -> list[dict]:
    """Company-list source, transparent to local vs GitHub Actions.

    Local dev reads live from the Notion desktop-app token. GitHub Actions has no
    desktop token, so it reads the static companies.json committed to the repo
    (regenerate it locally with export_companies and commit when the list changes).
    Set USE_STATIC_COMPANIES=1 to force the static file even locally.

    get_companies() already applies URL_OVERRIDES, so the exported file is
    post-override and needs no further fix-ups.
    """
    cj = BASE_DIR / "companies.json"
    if os.getenv("USE_STATIC_COMPANIES") != "1":
        token = get_notion_token()
        if token:
            try:
                return get_companies(token, get_schema(token))
            except Exception as e:
                log.warning(f"Notion read failed ({e}); falling back to companies.json")
    if cj.exists():
        log.info("Loading companies from static companies.json")
        return json.loads(cj.read_text())
    return []


# ─── NOTION: update posting status ────────────────────────────────────────────
def update_posting_status(token: str, page_id: str, status: str):
    headers = {"Cookie": f"token_v2={token}", "Content-Type": "application/json"}
    requests.post(
        "https://www.notion.so/api/v3/saveTransactions",
        headers=headers,
        json={
            "requestId": f"update-{page_id[:8]}",
            "transactions": [{
                "id": f"txn-{page_id[:8]}",
                "spaceId": NOTION_SPACE_ID,
                "operations": [{
                    "pointer":  {"table": "block", "id": page_id, "spaceId": NOTION_SPACE_ID},
                    "command":  "set",
                    "path":     ["properties", "jQio"],  # Posting Status property ID
                    "args":     [[status]],
                }],
            }],
        },
        timeout=15,
    )


# ─── SCRAPER ─────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# Cap generously so long job-listing pages (big employers) aren't truncated
# mid-list — don't-miss priority. The LLM careers eval still trims its own input.
_TEXT_CAP = 25000

def _html_to_text(html: str) -> str:
    """Strip chrome tags and return visible page text (capped)."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "meta"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)[:_TEXT_CAP]

# Per-fetch cache of job-description text keyed by signature (e.g. "gh:12345").
# Populated by structured adapters that carry full JD content (Greenhouse
# content=true, Workable, Lever). The careers loop uses it to run the same
# experience filter it applies to LinkedIn — otherwise a senior role with a
# junior-looking title (e.g. "Quantitative Researcher" needing 5 yrs) slips
# through, since the title alone hides the experience bar.
_JD_TEXT: dict[str, str] = {}
# signature → real per-job apply URL. Careers alerts link to this specific job
# instead of the company's (often non-human, API-endpoint) careers URL.
_JOB_URL: dict[str, str] = {}
# signature → posting date (absolute "YYYY-MM-DD" where available, else the
# feed's relative text like "Posted 5 Days Ago"). Populated best-effort by the
# structured adapters; used to show/rank postings by freshness.
_JOB_DATE: dict[str, str] = {}


def human_url(url: str) -> str:
    """Map a monitored API/endpoint URL to a browser-openable landing page.

    Many companies are monitored via a backend API endpoint (Greenhouse
    boards-api, TalentBrew /search-jobs/results, Standard Chartered's Radancy
    service, …). Opening those in a browser shows raw JSON (乱码) or a blank
    shell. This is used ONLY for the fallback alert link (when no per-job link
    matched), so the user always lands on a real, human careers/search page.
    URLs that are already human pages fall through unchanged."""
    if not url:
        return url
    # Greenhouse board API → hosted job board
    m = re.search(r"boards-api\.greenhouse\.io/v1/boards/([^/?]+)", url)
    if m:
        return f"https://job-boards.greenhouse.io/{m.group(1)}"
    # TalentBrew/Radancy results endpoint → its human search page
    if "/search-jobs/results" in url:
        return url.split("/search-jobs/results")[0] + "/search-jobs"
    # Standard Chartered Radancy service API → search page
    if "jobs.standardchartered.com/services/recruiting" in url:
        return "https://jobs.standardchartered.com/search-jobs"
    # Workable widget API → hosted careers page
    m = re.search(r"apply\.workable\.com/api/v1/widget/accounts/([^/?]+)", url)
    if m:
        return f"https://apply.workable.com/{m.group(1)}/"
    # Lever postings API → hosted job board
    m = re.search(r"api\.lever\.co/v0/postings/([^/?]+)", url)
    if m:
        return f"https://jobs.lever.co/{m.group(1)}"
    # Workday CXS API → human myworkdayjobs site (…/wday/cxs/{t}/{site}/jobs)
    m = re.search(r"(https?://[^/]+)/wday/cxs/([^/]+)/([^/]+)/jobs", url)
    if m:
        host = m.group(1).replace("wd", "wd", 1)  # host already the wd host
        return f"{host}/en-US/{m.group(3)}"
    # Bank of America jobs servlet → careers search
    if "careers.bankofamerica.com/services/jobssearchservlet" in url:
        return "https://careers.bankofamerica.com/en-us/search"
    # Standard Chartered / other Radancy service endpoints already handled above.
    return url
# normalised anchor text → job-page href, harvested from generic HTML/rendered
# careers pages (companies without a structured adapter). The careers loop maps
# these onto text-signature jobs so alerts link to the posting, not the search
# page. Cleared per fetch alongside _JOB_URL.
_PAGE_LINKS: dict[str, str] = {}
# Hrefs that look like a specific job posting (not nav/search/marketing).
_JOB_HREF_RE = re.compile(
    r"(/job[/-]|/jobs/|gh_jid=|/vacanc|/position|/opening|/posting|/careers/job|"
    r"/hrt-job/|myworkdayjobs\.com/.+/job/|eightfold\.ai/careers/job|"
    r"ashbyhq\.com/[^/]+/[0-9a-f]{8}|greenhouse\.io/[^/]+/jobs/\d)",
    re.I,
)

def _norm_title(s: str) -> str:
    """Normalise a job title for fuzzy matching anchor text ↔ signature."""
    s = s.split(" | ")[0]                      # drop " | location" suffix
    return re.sub(r"[^a-z0-9]", "", s.lower())

def _clean_anchor_text(text: str) -> str:
    """Collapse whitespace and strip a leading TalentBrew-style '{id} {date} '
    prefix (e.g. '96786789424 07/22/2026 Senior Associate…') so the anchor text
    matches the extracted job-title signature."""
    t = re.sub(r"\s+", " ", text).strip()
    t = re.sub(r"^\d{4,}\s+\d{1,2}/\d{1,2}/\d{2,4}\s*", "", t)   # id + date prefix
    return t

def _harvest_links(pairs) -> None:
    """Store (anchor_text, href) pairs that point at a specific posting."""
    for text, href in pairs:
        if not href or not text:
            continue
        clean = _clean_anchor_text(text)
        if len(clean) < 8 or not _JOB_HREF_RE.search(href):
            continue
        _PAGE_LINKS.setdefault(_norm_title(clean), href.split("#")[0])

def _jd_plain(html: str) -> str:
    """Flatten a JD HTML blob to plain text for the experience regex/LLM."""
    if not html:
        return ""
    txt = BeautifulSoup(html, "html.parser").get_text(separator=" ", strip=True)
    # Keep the whole JD (bounded) — the experience/qualifications section that
    # states "N years" usually sits at the END, so a short cap would miss it.
    return re.sub(r"\s+", " ", txt)[:12000]


# ─── HEADLESS BROWSER FALLBACK (for JS-rendered careers pages) ────────────────
_browser    = None   # lazy Playwright browser singleton — reused across pages
_playwright = None

def _get_browser():
    global _browser, _playwright
    if _browser is None:
        from playwright.sync_api import sync_playwright
        _playwright = sync_playwright().start()
        _browser = _playwright.chromium.launch(headless=True)
    return _browser

def fetch_text_rendered(url: str) -> str | None:
    """Render a JS page with headless Chromium, return its visible text.

    Used as a fallback when a plain HTTP fetch yields an empty/near-empty
    shell (single-page-app careers pages that load jobs via JavaScript).
    Waits for network to settle then a fixed beat for client-side hydration,
    and pulls text from every frame so jobs inside embedded widget iframes
    (Phenom, Avature, Ashby, …) are captured too.
    """
    try:
        page = _get_browser().new_page()
        # Bound EVERY Playwright call so a busy/polling page can never hang the run
        page.set_default_timeout(15000)
        try:
            page.goto(url, timeout=25000, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=6000)
            except Exception:
                pass
            page.wait_for_timeout(2000)   # let client-side templating hydrate
            # Main document + up to a few child frames (iframe-embedded job widgets).
            # Each content() is bounded by the default timeout above.
            parts = []
            for frame in page.frames[:8]:
                try:
                    parts.append(frame.content())
                except Exception:
                    pass
                # Harvest (anchor text, absolute href) so alerts can link to the
                # specific posting instead of the company's search page.
                try:
                    _harvest_links(frame.eval_on_selector_all(
                        "a[href]", "els => els.map(e => [e.innerText, e.href])"))
                except Exception:
                    pass
        finally:
            page.close()
        text = "\n".join(_html_to_text(h) for h in parts if h)
        return text[:_TEXT_CAP] if text else None
    except Exception as e:
        log.warning(f"render failed {url}: {e}")
        return None

def close_browser():
    """Tear down the shared browser at the end of a run."""
    global _browser, _playwright
    if _browser is not None:
        try:
            _browser.close()
            _playwright.stop()
        except Exception:
            pass
        _browser = _playwright = None


# ─── WORKDAY ADAPTER (big banks / asset managers) ────────────────────────────
# Big employers on Workday list thousands of mostly-irrelevant roles, so we
# query the CXS API with targeted search terms rather than pulling everything.
# Seniority/location is still filtered downstream by the LLM. Job id = the
# immutable req id (bulletFields[0]); signature prefix [wd:<id>].
_WD_SEARCHES = ["quantitative", "graduate", "index"]

def fetch_workday(cxs_url: str) -> str | None:
    """Pull relevant postings from a Workday CXS jobs endpoint.

    cxs_url looks like
    https://{tenant}.wd{N}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs
    """
    hdrs = {"User-Agent": HEADERS["User-Agent"], "Content-Type": "application/json",
            "Accept": "application/json"}
    jobs = {}
    try:
        def _page(term, offset):
            body = json.dumps({"appliedFacets": {}, "limit": 20,
                               "offset": offset, "searchText": term})
            r = requests.post(cxs_url, data=body, headers=hdrs, timeout=20)
            return r.json() if r.status_code == 200 else None

        # Small employers (≤500 reqs) → pull EVERYTHING with an empty search, so
        # trading/analyst/commodities roles aren't missed by the quant/graduate/
        # index keyword filter (e.g. Castleton's Commodities Trading Analyst
        # programmes). Big employers (banks, thousands of mostly-retail reqs) →
        # keep the targeted keyword search to avoid pulling the whole site.
        probe = _page("", 0)
        total = probe.get("total", 0) if probe else 0
        terms = [""] if (probe and 0 < total <= 500) else _WD_SEARCHES

        # Human-viewable base for per-job links: host/en-US/{site} + externalPath
        m = re.match(r"(https?://[^/]+)/wday/cxs/[^/]+/([^/]+)/jobs", cxs_url)
        human_base = f"{m.group(1)}/en-US/{m.group(2)}" if m else None

        for term in terms:
            offset = 0
            # High bound; `offset >= total` break below stops at the real total.
            # Even narrow terms exceed 200 at big employers (State Street ~215),
            # so a 200 cap would truncate & miss jobs beyond it.
            while offset < 600:
                data = _page(term, offset)
                if not data:
                    break
                posts = data.get("jobPostings", [])
                for p in posts:
                    bf  = p.get("bulletFields") or []
                    jid = bf[0] if bf else p.get("externalPath", "")
                    if jid:
                        jobs[jid] = f"[wd:{jid}] {p.get('title','')} | {p.get('locationsText','')}"
                        ep = p.get("externalPath", "")
                        if human_base and ep:
                            _JOB_URL[f"wd:{jid}"] = human_base + ep
                        _po = p.get("postedOn") or p.get("startDate") or ""
                        if _po:
                            _JOB_DATE[f"wd:{jid}"] = str(_po)
                offset += 20
                if offset >= data.get("total", 0):
                    break
        return "\n".join(jobs.values()) if jobs else ""
    except Exception as e:
        log.warning(f"workday fetch failed {cxs_url}: {e}")
        return None


# ─── STANDARD CHARTERED ADAPTER (Radancy recruiting API) ─────────────────────
# HK/Asia-heavy bank. POST search API keyed on target terms; job id is stable.
_SC_SEARCHES = ["quantitative", "graduate", "markets", "trading", "index"]

def fetch_standardchartered(api_url: str) -> str | None:
    """Pull relevant postings from Standard Chartered's Radancy jobs API."""
    hdrs = {"User-Agent": HEADERS["User-Agent"], "Content-Type": "application/json",
            "Accept": "application/json"}
    jobs = {}
    try:
        for term in _SC_SEARCHES:
            for pg in range(0, 40):   # high bound; `if not results: break` stops at real total
                body = json.dumps({"locale": "en_GB", "pageNumber": pg, "sortBy": "",
                                   "keywords": term, "location": "", "facetFilters": {},
                                   "brand": "", "skills": [], "categoryId": 0,
                                   "alertId": "", "rcmCandidateId": ""})
                resp = requests.post(api_url, data=body, headers=hdrs, timeout=20)
                if resp.status_code != 200:
                    break
                results = resp.json().get("jobSearchResult", [])
                if not results:
                    break
                for item in results:
                    r     = item.get("response", {})
                    jid   = r.get("id", "")
                    title = r.get("unifiedStandardTitle", "")
                    loc   = ", ".join(r.get("jobLocationShort", []))
                    if jid:
                        jobs[jid] = f"[sc:{jid}] {title} | {loc}"
                        # TalentBrew detail route: /job/{urlTitle}/{id} — the SPA
                        # reads the id from the path and renders the exact posting.
                        slug = r.get("urlTitle") or r.get("unifiedUrlTitle") or ""
                        _JOB_URL[f"sc:{jid}"] = (
                            f"https://jobs.standardchartered.com/job/{slug}/{jid}"
                            if slug else
                            f"https://jobs.standardchartered.com/job/{jid}")
        return "\n".join(jobs.values()) if jobs else ""
    except Exception as e:
        log.warning(f"standardchartered fetch failed: {e}")
        return None


# ─── BANK OF AMERICA ADAPTER (custom jobs servlet) ───────────────────────────
# The servlet has no keyword search — it returns all ~1900 postings — so we
# pull everything once and filter by title to the user's target functions.
_BANK_TITLE_RE = re.compile(
    r"\b(quant|quantitative|graduate|markets|trading|trader|index|systematic|"
    r"derivativ|strats|strategist|electronic trading)\b", re.I)

def fetch_bofa(servlet_url: str) -> str | None:
    """Pull BofA postings (all, then title-filter to relevant functions)."""
    try:
        url  = f"{servlet_url}?start=0&rows=5000&search=getAllJobs"   # 1883 now; headroom so growth past 2000 isn't truncated
        resp = requests.get(url, headers={**HEADERS, "Accept": "application/json"}, timeout=25)
        if resp.status_code != 200:
            return None
        jobs = {}
        for j in resp.json().get("jobsList", []) or []:
            title = j.get("postingTitle", "")
            if not _BANK_TITLE_RE.search(title):
                continue
            jid = j.get("jobRequisitionId", "")
            loc = j.get("locationString") or j.get("location", "")
            if jid:
                jobs[jid] = f"[ba:{jid}] {title} | {loc}"
                jcr = j.get("jcrURL") or ""
                if jcr:
                    _JOB_URL[f"ba:{jid}"] = "https://careers.bankofamerica.com" + jcr
        return "\n".join(jobs.values()) if jobs else ""
    except Exception as e:
        log.warning(f"bofa fetch failed: {e}")
        return None


# ─── DEUTSCHE BANK ADAPTER (beesite search API) ──────────────────────────────
_BANK_SEARCHES = ["quantitative", "graduate", "markets", "trading", "index"]

def fetch_deutschebank(base_url: str) -> str | None:
    """Pull relevant DB postings via the beesite search API (GET, data= JSON)."""
    jobs = {}
    try:
        for term in _BANK_SEARCHES:
            data = {"LanguageCode": "en",
                    "SearchParameters": {"FirstItem": 1, "CountItem": 100,
                        "MatchedObjectDescriptor": ["PositionTitle", "PositionID",
                            "PositionLocation.CityName", "PositionLocation.CountryName",
                            "ApplyURI"],
                        "Sort": [{"Criterion": "PublicationStartDate", "Direction": "DESC"}]},
                    "SearchCriteria": [{"CriterionName": "PositionFormattedDescription.Content",
                                        "CriterionValue": term}]}
            u = base_url + "?data=" + urllib.parse.quote(json.dumps(data))
            resp = requests.get(u, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                continue
            for it in resp.json().get("SearchResult", {}).get("SearchResultItems", []) or []:
                d   = it.get("MatchedObjectDescriptor", {})
                jid = d.get("PositionID", "")
                locs = d.get("PositionLocation", []) or []
                loc = ", ".join(l.get("CityName", "") for l in locs[:2] if l.get("CityName"))
                if jid:
                    jobs[jid] = f"[db:{jid}] {d.get('PositionTitle','')} | {loc}"
                    # ApplyURI is a full Workday URL ending in /apply; trim it to
                    # the job-view page so the email links to a readable posting.
                    apply = d.get("ApplyURI")
                    if isinstance(apply, list):
                        apply = apply[0] if apply else ""
                    if apply:
                        _JOB_URL[f"db:{jid}"] = apply[:-6] if apply.endswith("/apply") else apply
        return "\n".join(jobs.values()) if jobs else ""
    except Exception as e:
        log.warning(f"deutschebank fetch failed: {e}")
        return None


# ─── GOLDMAN SACHS ADAPTER (higher.gs.com GraphQL) ───────────────────────────
_GS_QUERY = ("query GetRoles($searchQueryInput: RoleSearchQueryInput!) { "
             "roleSearch(searchQueryInput: $searchQueryInput) { totalCount "
             "items { roleId jobTitle locations { city country } } } }")

def fetch_goldman(api_url: str) -> str | None:
    """Pull Goldman roles via GraphQL: all early-career + target-term searches."""
    hdrs = {"User-Agent": HEADERS["User-Agent"], "Content-Type": "application/json",
            "Accept": "application/json"}
    jobs = {}
    # ("", EARLY_CAREER) grabs every new-grad role; term searches add quant/etc.
    searches = [("", ["EARLY_CAREER"])] + \
               [(t, ["EARLY_CAREER", "PROFESSIONAL"]) for t in _BANK_SEARCHES]
    try:
        for term, exps in searches:
            page = 0
            # High bound; the totalCount break below stops at the real total.
            # Broad terms exceed 160 at Goldman (markets ~740, trading ~409,
            # even quantitative ~305), so 8 pages would truncate & miss new jobs.
            while page < 40:
                payload = {"operationName": "GetRoles", "query": _GS_QUERY,
                    "variables": {"searchQueryInput": {
                        "page": {"pageSize": 20, "pageNumber": page},
                        "sort": {"sortStrategy": "RELEVANCE", "sortOrder": "DESC"},
                        "filters": [], "experiences": exps, "searchTerm": term}}}
                resp = requests.post(api_url, data=json.dumps(payload), headers=hdrs, timeout=20)
                if resp.status_code != 200:
                    break
                rs = resp.json().get("data", {}).get("roleSearch", {})
                for it in rs.get("items", []) or []:
                    jid = it.get("roleId", "")
                    loc = ", ".join(l.get("city", "") for l in (it.get("locations") or [])[:2] if l.get("city"))
                    if jid:
                        jobs[jid] = f"[gs:{jid}] {it.get('jobTitle','')} | {loc}"
                        # roleId is "162057_GS_EARLY_CAREER"; the numeric prefix is
                        # the real posting id at higher.gs.com/roles/{num}.
                        num = jid.split("_")[0]
                        _JOB_URL[f"gs:{jid}"] = f"https://higher.gs.com/roles/{num}"
                if (page + 1) * 20 >= rs.get("totalCount", 0):
                    break
                page += 1
        return "\n".join(jobs.values()) if jobs else ""
    except Exception as e:
        log.warning(f"goldman fetch failed: {e}")
        return None


# ─── ORACLE CLOUD HCM ADAPTER (JPMorgan & others) ────────────────────────────
# Stored URL carries the base endpoint + ?siteNumber=CX_xxxx. Job id = req Id.
def fetch_oracle(base_url: str) -> str | None:
    """Pull relevant postings from an Oracle Cloud recruiting API."""
    m = re.search(r"siteNumber=([A-Za-z0-9_]+)", base_url)
    if not m:
        return None
    site = m.group(1)
    endpoint = base_url.split("?")[0]
    host = re.match(r"(https?://[^/]+)", base_url).group(1)
    jobs = {}
    # Only surface postings from the last 48h (same freshness window as LinkedIn).
    # PostedDate is date-only, so use the calendar date 48h ago as the cutoff —
    # inclusive, so anything posted in the last two days is kept (recall-first).
    cutoff = (datetime.now() - timedelta(hours=48)).date()
    try:
        for term in _BANK_SEARCHES:
            offset = 0
            stop = False
            # POSTING_DATES_DESC → newest first, so we can stop paginating as soon
            # as we cross the 48h cutoff. offset<400 is just a safety bound.
            while offset < 400 and not stop:
                u = (f"{endpoint}?onlyData=true&expand=requisitionList.secondaryLocations"
                     f"&finder=findReqs;siteNumber={site},keyword={urllib.parse.quote(term)},"
                     f"sortBy=POSTING_DATES_DESC,limit=50,offset={offset}")
                resp = requests.get(u, headers={**HEADERS, "Accept": "application/json"}, timeout=20)
                if resp.status_code != 200:
                    break
                data  = resp.json().get("items", [{}])
                reqs  = data[0].get("requisitionList", []) if data else []
                if not reqs:
                    break
                for it in reqs:
                    posted = str(it.get("PostedDate", ""))[:10]
                    try:
                        pdate = datetime.strptime(posted, "%Y-%m-%d").date()
                    except ValueError:
                        pdate = None
                    if pdate and pdate < cutoff:
                        stop = True          # list is date-desc → everything after is older
                        break
                    jid = it.get("Id", "")
                    if jid:
                        jobs[jid] = f"[or:{jid}] {it.get('Title','')} | {it.get('PrimaryLocation','')}"
                        _JOB_URL[f"or:{jid}"] = f"{host}/hcmUI/CandidateExperience/en/sites/{site}/job/{jid}"
                if len(reqs) < 50:
                    break
                offset += 50
        return "\n".join(jobs.values()) if jobs else ""
    except Exception as e:
        log.warning(f"oracle fetch failed: {e}")
        return None


# ─── EIGHTFOLD AI ADAPTER (Morgan Stanley & others) ──────────────────────────
def fetch_eightfold(base_url: str) -> str | None:
    """Pull relevant postings from an Eightfold pcsx search API.

    base_url like https://{tenant}.eightfold.ai/api/pcsx/search?domain=...
    Some tenants (e.g. Millennium) 403 the pcsx/search endpoint but serve the
    same catalogue via /api/apply/v2/jobs — handled by its own branch below.
    """
    jobs = {}
    _m = re.match(r"(https?://[^/]+)", base_url)
    ef_host = _m.group(1) if _m else ""
    _dm = re.search(r"[?&]domain=([^&]+)", base_url)
    ef_domq = f"?domain={_dm.group(1)}" if _dm else ""

    # apply/v2/jobs variant: top-level `positions`, no query term, paginate by
    # start/num; canonicalPositionUrl + t_create carry the link/date.
    if "/api/apply/v2/jobs" in base_url:
        try:
            start = 0
            while start < 2000:
                sep = "&" if "?" in base_url else "?"
                u = f"{base_url}{sep}start={start}&num=10"
                resp = requests.get(u, headers={**HEADERS, "Accept": "application/json"}, timeout=20)
                if resp.status_code != 200:
                    break
                data = resp.json()
                positions = data.get("positions", []) or []
                total = data.get("count", 0)
                if not positions:
                    break
                for pos in positions:
                    jid = pos.get("id", "")
                    loc = ", ".join(pos.get("locations", [])[:2]) or pos.get("location", "")
                    if jid:
                        jobs[jid] = f"[ef:{jid}] {pos.get('name','')} | {loc}"
                        purl = pos.get("canonicalPositionUrl") or ""
                        if purl:
                            _JOB_URL[f"ef:{jid}"] = purl
                        _ts = pos.get("t_create") or pos.get("t_update")
                        if _ts:
                            try:
                                _JOB_DATE[f"ef:{jid}"] = datetime.fromtimestamp(int(_ts)).strftime("%Y-%m-%d")
                            except Exception:
                                pass
                start += len(positions)
                if total and start >= total:
                    break
            return "\n".join(jobs.values()) if jobs else ""
        except Exception as e:
            log.warning(f"eightfold apply/v2 fetch failed: {e}")
            return None
    try:
        for term in _BANK_SEARCHES:
            start = 0
            # High bound; the len(positions)<10 break below stops at the real
            # total. Broad terms (markets/trading) exceed 200 at big banks
            # (e.g. Morgan Stanley ~500+), so 200 would truncate & miss new jobs.
            while start < 800:
                sep = "&" if "?" in base_url else "?"
                u = f"{base_url}{sep}query={urllib.parse.quote(term)}&location=&start={start}&num=10"
                resp = requests.get(u, headers={**HEADERS, "Accept": "application/json"}, timeout=20)
                if resp.status_code != 200:
                    break
                positions = resp.json().get("data", {}).get("positions", []) or []
                if not positions:
                    break
                for pos in positions:
                    jid = pos.get("id", "")
                    loc = ", ".join(pos.get("locations", [])[:2])
                    if jid:
                        jobs[jid] = f"[ef:{jid}] {pos.get('name','')} | {loc}"
                        # Eightfold returns a canonical positionUrl (/careers/job/ID)
                        purl = pos.get("positionUrl") or ""
                        if purl:
                            _JOB_URL[f"ef:{jid}"] = ef_host + purl + ef_domq
                        _ts = pos.get("postedTs") or pos.get("creationTs")
                        if _ts:
                            try:
                                _JOB_DATE[f"ef:{jid}"] = datetime.fromtimestamp(int(_ts)).strftime("%Y-%m-%d")
                            except Exception:
                                pass
                if len(positions) < 10:
                    break
                start += 10
        return "\n".join(jobs.values()) if jobs else ""
    except Exception as e:
        log.warning(f"eightfold fetch failed: {e}")
        return None


def fetch_hrt_sitemap(url: str) -> str | None:
    """Pull all HRT postings from its Yoast job sitemap.

    HRT's careers/landing pages are JS-rendered marketing; the real postings
    live under /hrt-job/{slug}/ and are enumerated in hrt_jobs-sitemap.xml.
    The slug is a stable per-job ID → signature [hrt:slug]. Keep ALL jobs
    (recall-first); the LLM filters relevance downstream.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return None
        lines = []
        for loc in re.findall(r"<loc>\s*([^<]*?/hrt-job/[^<]+?)\s*</loc>", resp.text):
            slug = loc.rstrip("/").rsplit("/hrt-job/", 1)[-1].strip("/")
            if not slug:
                continue
            title = slug.replace("-", " ").title()
            lines.append(f"[hrt:{slug}] {title}")
            # The <loc> IS the human job page (/hrt-job/{slug}/) — link emails to it
            # instead of falling back to the raw sitemap XML URL (unopenable).
            _JOB_URL[f"hrt:{slug}"] = loc.rstrip("/") + "/"
        return "\n".join(lines) if lines else ""
    except Exception as e:
        log.warning(f"hrt sitemap fetch failed: {e}")
        return None


def fetch_talentbrew(url: str) -> str | None:
    """TalentBrew / Radancy careers sites (BlackRock, Citi, Barclays, Capital
    One, Charles Schwab, ING). Their /search-jobs page renders only ~15 jobs in
    the browser, but the underlying /search-jobs/results endpoint returns 100
    postings per page as a JSON-wrapped HTML fragment — so we PAGINATE it to
    pull the WHOLE listing instead of one page. Each card carries a stable job
    id, title, location and a real /job/... URL (→ _JOB_URL), fixing both the
    coverage gap (single page → whole feed) and link precision (specific job
    page instead of the search-page fallback).

    The monitored URL is `https://{host}/search-jobs/results`; the host alone
    identifies the company (the job URLs carry the site's own numeric id).
    """
    m = re.match(r"(https?://[^/]+)", url)
    if not m:
        return None
    host     = m.group(1)
    endpoint = host + "/search-jobs/results"
    headers  = {**HEADERS, "X-Requested-With": "XMLHttpRequest", "Accept": "application/json"}
    lines, seen_ids = [], set()
    try:
        for page in range(1, 41):          # cap 40×100 = 4000 jobs
            params = {
                "ActiveFacetID": 0, "CurrentPage": page, "RecordsPerPage": 100,
                "Distance": 50, "RadiusUnitType": 0, "Keywords": "", "Location": "",
                "ShowRadius": "False", "IsPagination": "False",
                "SearchResultsModuleName": "Search Results",
                "SearchFiltersModuleName": "Search Filters",
                "SortCriteria": 1, "SortDirection": 1, "SearchType": 5,
            }
            resp = requests.get(endpoint, params=params, headers=headers, timeout=25)
            if resp.status_code != 200:
                break
            frag = (resp.json() or {}).get("results", "") or ""
            # Job-card HTML varies across TalentBrew instances, so parse structurally:
            # each posting is an <a href="/job/.../{jobid}"> whose title is the inner
            # <h2>/<h3> (or aria-label), plus a .job-location span.
            soup  = BeautifulSoup(frag, "html.parser")
            cards = soup.select('a[href*="/job/"]')
            new_on_page = 0
            for a in cards:
                href = a.get("href", "")
                jid  = a.get("data-job-id") or ""
                if not jid:
                    mj = re.search(r"/(\d+)(?:[/?#]|$)", href)
                    jid = mj.group(1) if mj else ""
                if not jid or jid in seen_ids:
                    continue
                seen_ids.add(jid); new_on_page += 1
                head  = a.find(["h1", "h2", "h3", "h4"])
                title = (head.get_text(" ", strip=True) if head
                         else a.get("aria-label") or a.get_text(" ", strip=True))
                title = re.sub(r"\s+", " ", title or "").strip()[:120]
                loc_el = a.find(class_=re.compile("job-location", re.I))
                loc    = loc_el.get_text(" ", strip=True) if loc_el else ""
                if not title:
                    continue
                lines.append(f"[tb:{jid}] {title}" + (f" | {loc}" if loc else ""))
                _JOB_URL[f"tb:{jid}"] = host + href.split("?")[0]
            if new_on_page == 0:           # no new postings → past the last page
                break
        return "\n".join(lines) if lines else ""
    except Exception as e:
        log.warning(f"talentbrew fetch failed {url}: {e}")
        return None


def fetch_greenhouse(url: str) -> str | None:
    """Greenhouse board API with content=true so JD text is available for the
    experience filter. URL like https://boards-api.greenhouse.io/v1/boards/{slug}/jobs
    """
    try:
        sep = "&" if "?" in url else "?"
        resp = requests.get(f"{url}{sep}content=true", headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return None
        # Slug from the board API URL → build the canonical Greenhouse-hosted job
        # URL. Some companies' `absolute_url` points at their own embedded board
        # (e.g. Tower: www.tower-research.com/open-positions/?gh_jid=… ) which does
        # NOT deep-link to the specific posting — it just shows the full list. The
        # hosted job-boards.greenhouse.io/{slug}/jobs/{id} URL always opens the
        # exact job in a browser, so prefer it.
        sm = re.search(r"/boards/([^/?]+)", url)
        slug = sm.group(1) if sm else ""
        lines = []
        for job in resp.json().get("jobs", []):
            jid = job.get("id", "")
            title = job.get("title", "")
            loc = (job.get("location") or {}).get("name", "")
            lines.append(f"[gh:{jid}] {title} | {loc}")
            _JD_TEXT[f"gh:{jid}"] = _jd_plain(job.get("content", ""))
            _pub = job.get("first_published") or job.get("updated_at") or ""
            if _pub:
                _JOB_DATE[f"gh:{jid}"] = _pub[:10]
            if slug and jid:
                _JOB_URL[f"gh:{jid}"] = f"https://job-boards.greenhouse.io/{slug}/jobs/{jid}"
            elif job.get("absolute_url"):
                _JOB_URL[f"gh:{jid}"] = job["absolute_url"]
        return "\n".join(lines) if lines else ""
    except Exception as e:
        log.warning(f"greenhouse fetch failed {url}: {e}")
        return None


def fetch_workable(url: str) -> str | None:
    """Workable widget API. URL like
    https://apply.workable.com/api/v1/widget/accounts/{slug}
    """
    try:
        sep = "&" if "?" in url else "?"
        resp = requests.get(f"{url}{sep}details=true", headers={**HEADERS, "Accept": "application/json"}, timeout=20)
        if resp.status_code != 200:
            return None
        lines = []
        for job in resp.json().get("jobs", []):
            sc = job.get("shortcode", "")
            title = job.get("title", "")
            loc = ", ".join(x for x in [job.get("city", ""), job.get("country", "")] if x)
            lines.append(f"[wk:{sc}] {title} | {loc}")
            _JD_TEXT[f"wk:{sc}"] = _jd_plain(job.get("description", ""))
            _pub = job.get("published_on") or job.get("created_at") or ""
            if _pub:
                _JOB_DATE[f"wk:{sc}"] = str(_pub)[:10]
            if job.get("shortlink") or job.get("url"):
                _JOB_URL[f"wk:{sc}"] = job.get("shortlink") or job.get("url")
        return "\n".join(lines) if lines else ""
    except Exception as e:
        log.warning(f"workable fetch failed {url}: {e}")
        return None


def fetch_lever(url: str) -> str | None:
    """Lever postings API. URL like https://api.lever.co/v0/postings/{slug}?mode=json"""
    try:
        resp = requests.get(url, headers={**HEADERS, "Accept": "application/json"}, timeout=20)
        if resp.status_code != 200:
            return None
        lines = []
        for job in resp.json():
            jid = job.get("id", "")
            title = job.get("text", "")
            loc = (job.get("categories") or {}).get("location", "")
            lines.append(f"[lv:{jid}] {title} | {loc}")
            _JD_TEXT[f"lv:{jid}"] = _jd_plain(job.get("descriptionPlain") or job.get("description", ""))
            _ca = job.get("createdAt")
            if _ca:
                try:
                    _JOB_DATE[f"lv:{jid}"] = datetime.fromtimestamp(int(_ca) / 1000).strftime("%Y-%m-%d")
                except Exception:
                    pass
            if job.get("hostedUrl") or job.get("applyUrl"):
                _JOB_URL[f"lv:{jid}"] = job.get("hostedUrl") or job.get("applyUrl")
        return "\n".join(lines) if lines else ""
    except Exception as e:
        log.warning(f"lever fetch failed {url}: {e}")
        return None


def fetch_ashby(slug: str) -> str | None:
    """Ashby job board posting API. slug from jobs.ashbyhq.com/{slug}.
    Returns {jobs:[{id,title,location,jobUrl,publishedAt,descriptionPlain,isListed}]}.
    """
    api = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=false"
    try:
        resp = requests.get(api, headers={**HEADERS, "Accept": "application/json"}, timeout=20)
        if resp.status_code != 200:
            return None
        lines = []
        for job in resp.json().get("jobs", []):
            if job.get("isListed") is False:
                continue
            jid = job.get("id", "")
            title = job.get("title", "")
            loc = job.get("location", "") or ""
            lines.append(f"[ash:{jid}] {title} | {loc}")
            _JD_TEXT[f"ash:{jid}"] = _jd_plain(job.get("descriptionPlain") or job.get("descriptionHtml", ""))
            _pub = job.get("publishedAt") or ""
            if _pub:
                _JOB_DATE[f"ash:{jid}"] = _pub[:10]
            if job.get("jobUrl") or job.get("applyUrl"):
                _JOB_URL[f"ash:{jid}"] = job.get("jobUrl") or job.get("applyUrl")
        return "\n".join(lines) if lines else ""
    except Exception as e:
        log.warning(f"ashby fetch failed {slug}: {e}")
        return None


def fetch_pinpoint(url: str) -> str | None:
    """Pinpoint ATS. URL like https://{slug}.pinpointhq.com/postings.json
    Returns {"data": [{id, title, location:{name}, description_..., url}]}.
    """
    try:
        resp = requests.get(url, headers={**HEADERS, "Accept": "application/json"}, timeout=20)
        if resp.status_code != 200:
            return None
        data = resp.json()
        items = data.get("data", data) if isinstance(data, dict) else data
        lines = []
        for job in items or []:
            jid = job.get("id", "")
            title = job.get("title", "")
            loc = job.get("location")
            loc = loc.get("name", "") if isinstance(loc, dict) else (loc or "")
            lines.append(f"[pp:{jid}] {title} | {loc}")
            jd = job.get("description") or job.get("description_body") or job.get("advert_body") or ""
            _JD_TEXT[f"pp:{jid}"] = _jd_plain(jd)
            if job.get("url"):
                _JOB_URL[f"pp:{jid}"] = job["url"]
        return "\n".join(lines) if lines is not None else ""
    except Exception as e:
        log.warning(f"pinpoint fetch failed {url}: {e}")
        return None


def fetch_sig(url: str) -> str | None:
    """Susquehanna (SIG) Jibe jobs API. URL like
    https://careers.sig.com/api/jobs — paginated 10/page under jobs[].data.
    """
    try:
        jobs = {}
        page = 1
        while page <= 40:
            u = re.sub(r"([?&])page=\d+", rf"\g<1>page={page}", url)
            if "page=" not in u:
                u += ("&" if "?" in u else "?") + f"page={page}"
            resp = requests.get(u, headers={**HEADERS, "Accept": "application/json"}, timeout=20)
            if resp.status_code != 200:
                break
            data = resp.json()
            page_jobs = data.get("jobs", [])
            if not page_jobs:
                break
            for wrap in page_jobs:
                j = wrap.get("data", wrap)
                jid = j.get("req_id") or j.get("slug", "")
                title = j.get("title", "")
                loc = j.get("full_location") or j.get("location_name", "")
                jobs[jid] = f"[sig:{jid}] {title} | {loc}"
                au = j.get("apply_url") or j.get("canonical_url")
                if au:
                    _JOB_URL[f"sig:{jid}"] = au
                _JD_TEXT[f"sig:{jid}"] = _jd_plain((j.get("description") or "") + " " + (j.get("qualifications") or ""))
            total = data.get("totalCount") or data.get("count") or 0
            if len(jobs) >= total or len(page_jobs) < 10:
                break
            page += 1
        return "\n".join(jobs.values()) if jobs else ""
    except Exception as e:
        log.warning(f"sig fetch failed {url}: {e}")
        return None


def fetch_kernel(url: str) -> str | None:
    """Kernel ATS (apptrkr.io) campaign API, e.g.
    https://api.apptrkr.io/api/v1/{company}/{campaign-slug}
    Returns {"data": {id, title, slug, status, campaign_fields:[...]}}.
    Rokos et al. run a single early-careers campaign here rather than a job list.
    """
    try:
        resp = requests.get(url, headers={**HEADERS, "Accept": "application/json"}, timeout=20)
        if resp.status_code != 200:
            return None
        d = resp.json().get("data")
        if not d:
            return ""
        items = d if isinstance(d, list) else [d]
        lines = []
        for c in items:
            if str(c.get("status")) != "1":     # skip closed campaigns (status is "1"/"0")
                continue
            cid = c.get("id") or c.get("slug", "")
            title = c.get("title", "")
            lines.append(f"[kn:{cid}] {title}")
            jd = " ".join((f.get("field_text") or "") for f in c.get("campaign_fields", []))
            _JD_TEXT[f"kn:{cid}"] = _jd_plain(jd)
        return "\n".join(lines) if lines else ""
    except Exception as e:
        log.warning(f"kernel fetch failed {url}: {e}")
        return None


def fetch_text(url: str) -> str | None:
    _JD_TEXT.clear()          # reset per-fetch JD cache
    _JOB_URL.clear()          # reset per-fetch job-URL cache
    _JOB_DATE.clear()         # reset per-fetch posting-date cache
    _PAGE_LINKS.clear()       # reset per-fetch harvested anchor links
    # Kernel ATS (apptrkr.io) campaign API
    if "api.apptrkr.io" in url:
        return fetch_kernel(url)
    # Susquehanna (SIG) Jibe jobs API
    if "careers.sig.com/api/jobs" in url:
        return fetch_sig(url)
    # Pinpoint ATS postings feed
    if "pinpointhq.com" in url and "postings" in url:
        return fetch_pinpoint(url)
    # HRT job sitemap (real postings behind a JS marketing page)
    if "hudsonrivertrading.com" in url and "hrt_jobs-sitemap.xml" in url:
        return fetch_hrt_sitemap(url)
    # TalentBrew/Radancy paginating results API (BlackRock, Citi, Barclays, …)
    if "/search-jobs/results" in url:
        return fetch_talentbrew(url)
    # Greenhouse board API (content=true → JD text for experience filter)
    if "boards-api.greenhouse.io" in url:
        return fetch_greenhouse(url)
    # Greenhouse HOSTED board page (job-boards.greenhouse.io/{slug}) — the
    # human-facing URL, not the API. Fetched as generic HTML it yields titles but
    # no per-job links/dates. Convert to the board API so we get structured data.
    _ghm = re.match(r"https?://job-boards\.greenhouse\.io/([^/?#]+)", url)
    if _ghm:
        return fetch_greenhouse(f"https://boards-api.greenhouse.io/v1/boards/{_ghm.group(1)}/jobs")
    # Ashby job board (jobs.ashbyhq.com/{slug} human page or the posting API).
    _asm = re.search(r"(?:jobs\.ashbyhq\.com|posting-api/job-board)/([^/?#]+)", url)
    if "ashbyhq.com" in url and _asm:
        return fetch_ashby(_asm.group(1))
    # Workable widget API
    if "apply.workable.com/api/" in url:
        return fetch_workable(url)
    # Lever postings API
    if "api.lever.co/v0/postings" in url:
        return fetch_lever(url)
    # Eightfold AI API (Morgan Stanley pcsx/search; Millennium apply/v2/jobs)
    if "eightfold.ai/api/pcsx/search" in url or "eightfold.ai/api/apply/v2/jobs" in url:
        return fetch_eightfold(url)
    # Workday CXS API (POST-based) — handled by its own adapter
    if "/wday/cxs/" in url and url.rstrip("/").endswith("/jobs"):
        return fetch_workday(url)
    # Workday HUMAN page (tenant.wdN.myworkdayjobs.com/[locale/]{site}[/...]) — the
    # JS SPA. A generic GET only yields page-1 titles with no links/dates/JD (and
    # no pagination), so companies configured with the human URL run degraded and
    # silently miss most jobs. Convert to the CXS API so fetch_workday pulls every
    # posting. site = first path segment after any locale; also handles /jobs tabs
    # and deep /job/… permalinks. (CXS form already returned above.)
    _wdm = re.match(
        r"https?://([^.]+)(\.wd\d+\.myworkdayjobs\.com)/(?:[a-z]{2}-[A-Z]{2}/)?([^/?#]+)",
        url,
    )
    if _wdm:
        tenant, dom, site = _wdm.group(1), _wdm.group(2), _wdm.group(3)
        # Force https — the CXS POST API rejects http (some configs use http://).
        return fetch_workday(f"https://{tenant}{dom}/wday/cxs/{tenant}/{site}/jobs")
    # Deutsche Bank beesite search API
    if "api-deutschebank.beesite.de/search" in url:
        return fetch_deutschebank(url)
    # Goldman Sachs GraphQL
    if "api-higher.gs.com" in url:
        return fetch_goldman(url)
    # Oracle Cloud HCM recruiting API (JPMorgan, …)
    if "oraclecloud.com/hcmRestApi" in url:
        return fetch_oracle(url)
    # Standard Chartered Radancy API (POST-based)
    if "jobs.standardchartered.com/services/recruiting" in url:
        return fetch_standardchartered(url)
    # Bank of America jobs servlet
    if "careers.bankofamerica.com/services/jobssearchservlet" in url:
        return fetch_bofa(url)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            # Blocked (403 anti-bot) or error status. A real browser often still
            # loads the page — so render before giving up rather than miss the
            # whole company silently (don't-miss priority). e.g. UBS/BNP/Manulife
            # 403 the plain request but render fine.
            log.info(f"  HTTP {resp.status_code}; trying headless render for {url}")
            return fetch_text_rendered(url)
        # Jane Street custom JSON feed (list of {id, position, city, …})
        if "janestreet.com/jobs/main.json" in url:
            try:
                data = resp.json()
                lines = []
                for j in data:
                    if not isinstance(j, dict):
                        continue
                    jid = j.get("id", "")
                    lines.append(f"[js:{jid}] {j.get('position','')} | {j.get('city','')}")
                    _JD_TEXT[f"js:{jid}"] = _jd_plain(j.get("overview", ""))
                    if jid:
                        _JOB_URL[f"js:{jid}"] = f"https://www.janestreet.com/join-jane-street/position/{jid}/"
                return "\n".join(lines) if lines else ""
            except Exception:
                pass
        # Greenhouse JSON API (boards-api.greenhouse.io)
        if "boards-api.greenhouse.io" in url or "application/json" in resp.headers.get("Content-Type", ""):
            try:
                data = resp.json()
                if "jobs" in data:
                    lines = []
                    for job in data["jobs"]:
                        job_id   = job.get("id", "")
                        title    = job.get("title", "")
                        location = job.get("location", {}).get("name", "")
                        # [gh:ID] prefix carries the immutable job ID used as signature
                        lines.append(f"[gh:{job_id}] {title} | {location}")
                    return "\n".join(lines) if lines else ""
            except Exception:
                pass
        http_text = _html_to_text(resp.text)
        # Harvest job-posting anchors from the raw HTML so alerts can link to the
        # specific posting rather than the company search page.
        try:
            _harvest_links(
                [(a.get_text(" ", strip=True), urllib.parse.urljoin(url, a["href"]))
                 for a in BeautifulSoup(resp.text, "html.parser").find_all("a", href=True)])
        except Exception:
            pass
        # Greenhouse-embedded HTML boards are already server-rendered — no need to render
        if "greenhouse.io" in url:
            return http_text
        # JS-rendered suspicion: sparse text / few job lines → render in a browser.
        # Bias toward rendering (don't-miss priority); only rich pages skip it.
        if len(extract_jobs(http_text)) < 5 or len(http_text) < 1500:
            rendered = fetch_text_rendered(url)
            if rendered and len(extract_jobs(rendered)) > len(extract_jobs(http_text)):
                return rendered
        return http_text
    except Exception as e:
        # Connection reset / TLS / timeout on the plain request — a headless
        # browser often still loads the page, so try it before giving up.
        log.warning(f"fetch failed {url}: {e}; trying headless render")
        try:
            return fetch_text_rendered(url)
        except Exception:
            return None


# ─── LINKEDIN JOB SEARCH ──────────────────────────────────────────────────────
# The guest API returns ≤10 per page (verified: 1765 single searches in the log
# all capped at 10). High-volume locations (esp. United States) have many pages of
# results in a 36h window, so a small fixed cap would truncate & miss them. We
# paginate up to _LI_PAGES but the real terminator is the early-stop in the loop
# (a page with no new job IDs / no cards), so small locations stop after 1-2 pages
# while the US keeps going until its results are exhausted.
_LI_PAGES = 45         # safety cap; US 36h volume ≈ 388/keyword, so cover it fully
_LI_PAGE_STEP = 10     # guest API 'start' increment

def search_linkedin_jobs(keyword: str, location: str = "") -> list[dict]:
    """Fetch LinkedIn job listings posted in the last 36h via their guest API.

    The guest API returns only ~10 results per request, so we PAGINATE (start=0,
    10, 20, …) instead of taking just the first page — otherwise relevant jobs
    ranked past the first page are silently missed (e.g. Fidelity 'Associate,
    Quant Data Operations' sat on page 2 under 'quantitative' and was never seen)."""
    li_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.linkedin.com/",
    }
    jobs, seen_ids = [], set()
    try:
        for page in range(_LI_PAGES):
            url = (
                "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
                f"?keywords={urllib.parse.quote(keyword)}"
                + (f"&location={urllib.parse.quote(location)}" if location else "")
                + "&f_TPR=r129600"   # last 36 hours
                + f"&start={page * _LI_PAGE_STEP}"
            )
            # Deep pagination = many rapid requests → LinkedIn rate-limits
            # (429/999/403), which returns 0 and would silently truncate. Pace the
            # requests and retry a throttled page once with backoff before giving up.
            resp = None
            for attempt in range(2):
                if page or attempt:
                    time.sleep(1.5 if attempt else 0.6)   # pacing / backoff
                resp = requests.get(url, headers=li_headers, timeout=20)
                if resp.status_code == 200:
                    break
            if resp is None or resp.status_code != 200:
                log.warning(f"LinkedIn throttled/{resp.status_code if resp else 'err'} "
                            f"for '{keyword}' @ {location} at page {page}; stopping pagination")
                break
            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.find_all("div", class_=re.compile(r"base-card"))
            if not cards:
                break   # no more results → stop paginating
            new_on_page = 0
            for card in cards:
                title_el    = card.find(class_=re.compile(r"base-search-card__title"))
                company_el  = card.find(class_=re.compile(r"base-search-card__subtitle"))
                location_el = card.find(class_=re.compile(r"job-search-card__location"))
                link_el     = card.find("a", href=re.compile(r"linkedin\.com/jobs/view"))
                time_el     = card.find("time")
                if not (title_el and company_el and link_el):
                    continue
                # Skip reposted jobs — they're old, not new openings
                if time_el and "repost" in (time_el.get("class") or []):
                    continue
                if card.find(string=re.compile(r"repost", re.I)):
                    continue
                href = link_el.get("href", "")
                # LinkedIn slug ends with the numeric job ID: …-slug-4429141266?…
                m = re.search(r"-(\d{7,12})(?:\?|$)", href)
                job_id = m.group(1) if m else href
                if job_id in seen_ids:
                    continue
                seen_ids.add(job_id); new_on_page += 1
                jobs.append({
                    "id":       job_id,
                    "title":    title_el.get_text(strip=True),
                    "company":  company_el.get_text(strip=True),
                    "location": location_el.get_text(strip=True) if location_el else "",
                    "url":      href.split("?")[0],
                    "posted":   time_el.get("datetime", "") if time_el else "",
                })
            if new_on_page == 0:
                break   # page had only dupes → past the useful results
        return jobs
    except Exception as e:
        log.warning(f"LinkedIn search error for '{keyword}': {e}")
        return jobs


def fetch_linkedin_jd(job_id: str) -> str:
    """Fetch full job description from LinkedIn's guest job posting API."""
    url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
    li_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.linkedin.com/",
    }
    try:
        resp = requests.get(url, headers=li_headers, timeout=15)
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        # Full JD lives in the show-more-less section
        jd_el = soup.find(class_=re.compile(r"show-more-less-html__markup"))
        if jd_el:
            return jd_el.get_text(separator="\n", strip=True)[:4000]
        # Fallback: strip all tags and return page text
        for tag in soup(["script", "style"]):
            tag.decompose()
        return soup.get_text(separator="\n", strip=True)[:4000]
    except Exception as e:
        log.warning(f"Failed to fetch LinkedIn JD for {job_id}: {e}")
        return ""


# ─── LLM CIRCUIT BREAKER ─────────────────────────────────────────────────────
# When DeepSeek has an incident, every eval fails its 5 retries (×3s sleep),
# ballooning the run so it may never finish/email. After enough consecutive
# full failures we "open" the breaker: each further eval makes ONE quick attempt
# (no retries/sleep), fails fast, and the job is held for retry next run — so the
# run still completes and sends its email. Any success closes the breaker.
_LLM_FAILS = 0
_LLM_CIRCUIT_OPEN = False
_LLM_FAIL_THRESHOLD = 8
# Set when the API rejects a call for billing reasons (402 / insufficient balance).
# This is a hard account problem, not a transient error — main() surfaces it as a
# distinct "top up balance" alert instead of a misleading "no new jobs" email.
_LLM_BALANCE_ERR = False

def _is_balance_error(e) -> bool:
    s = str(e)
    return "Insufficient Balance" in s or "402" in s or "invalid_request_error" in s

def _note_llm_error(e) -> bool:
    """Flag billing failures. Returns True if this is a balance error (caller
    should stop retrying — it won't recover mid-run)."""
    global _LLM_BALANCE_ERR
    if _is_balance_error(e):
        _LLM_BALANCE_ERR = True
        return True
    return False

def _llm_attempts() -> int:
    return 1 if _LLM_CIRCUIT_OPEN else 5

def _llm_note_success() -> None:
    global _LLM_FAILS, _LLM_CIRCUIT_OPEN
    _LLM_FAILS = 0
    _LLM_CIRCUIT_OPEN = False

def _llm_note_failure() -> None:
    global _LLM_FAILS, _LLM_CIRCUIT_OPEN
    _LLM_FAILS += 1
    if _LLM_FAILS >= _LLM_FAIL_THRESHOLD and not _LLM_CIRCUIT_OPEN:
        _LLM_CIRCUIT_OPEN = True
        log.warning(f"LLM circuit OPEN after {_LLM_FAILS} consecutive failures — "
                    f"fail-fast for the rest of this run, jobs held for retry")


def evaluate_linkedin_job(job: dict) -> dict | None:
    """Fetch the full JD then ask LLM whether this posting is relevant."""
    if not DEEPSEEK_KEY:
        return None

    # Quick pre-filter on title before spending an API call
    if SENIOR_TITLE_RE.search(job["title"]):
        return None

    # Fetch the actual job description
    jd_text = fetch_linkedin_jd(job["id"])
    if not jd_text:
        log.warning(f"No JD fetched for {job['company']} — {job['title']}, skipping")
        return None

    # Hard pre-filter: reject JDs whose minimum required experience is 3 or more years
    if exp_floor_ge3(jd_text):
        log.info(f"  Pre-filter (exp req): {job['company']} — {job['title']}")
        return None

    client = OpenAI(api_key=DEEPSEEK_KEY, base_url="https://api.deepseek.com")
    prompt = f"""Evaluate if this LinkedIn job posting is relevant to this candidate.
Read the FULL job description carefully — pay special attention to experience requirements.

Company: {job['company']}
Title: {job['title']}
Location: {job['location']}
Posted: {job['posted']}

--- JOB DESCRIPTION START ---
{jd_text}
--- JOB DESCRIPTION END ---

Candidate profile:
{USER_PROFILE}

Apply these checks in order. If ANY fail, set relevant=false:
1. Title check: senior/management seniority words (Director, VP, Vice President, Head of, Principal, Chief, Managing Director, Partner, Executive)? -> not relevant. ("Senior"/"Lead"/"Manager" alone is NOT an automatic reject — judge by the years requirement in check 2.)
2. Experience check — judge by the UPPER bound of the required experience.
   -> NOT relevant if EITHER: (a) a stated range whose UPPER bound is 4 or more years ("2-5 years", "2-4 years", "3-7 years", "1-5 years", "2 to 6 years"); OR (b) a minimum-only figure whose floor is 3 or more ("3+ years", "minimum 3 years", "at least 3 years", "5+ years").
   -> KEEP if ANY: a range whose upper bound is 3 or less ("0-2", "1-3", "0-3", "up to 3 years"); OR a minimum-only figure with floor 0-2 and NO upper bound ("1+ years", "2+ years", vague "prior experience"/"background in" with no number); OR new grad / no experience stated.
3. Internship check: internship/co-op/temporary? -> not relevant.
4. Location check: not in US (NYC, Chicago, SF, Boston, Stamford CT), UK/London, Hong Kong, Singapore, or Netherlands (remote-only also ok)? -> not relevant. (Australia/New Zealand are NOT targeted — treat AU/NZ locations as not relevant.)
5. Role check: keep ONLY if the role is quantitative research, quantitative/systematic trading,
   quantitative developer, quantitative analyst, index (analyst/researcher), or portfolio
   management. ANY software-engineering role is NOT relevant — "Software Engineer", "Software
   Developer", Java/Python/C++/C#/full-stack/backend/frontend/web/mobile/DevOps/platform,
   EVEN "for trading systems", low-latency, or at a quant/trading firm. (A title that is
   literally "Quantitative Developer"/"Quant Developer" stays in; "Software Engineer" does
   NOT.) If not one of the wanted quant/trading/index/portfolio functions -> not relevant.

Reply ONLY in this JSON format:
{{
  "relevant": true or false,
  "reason": "one sentence citing the specific JD text that made you decide",
  "roles": ["role name"]
}}"""

    last_err = None
    attempts = _llm_attempts()
    for attempt in range(attempts):
        try:
            msg = client.chat.completions.create(
                model="deepseek-v4-flash",
                max_tokens=8192,   # reasoner burns tokens on reasoning first; too low → empty content
                messages=[{"role": "user", "content": prompt}],
            )
            text = msg.choices[0].message.content.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
                if text.startswith("json"):
                    text = text[4:].strip()
            parsed = json.loads(text)
            _llm_note_success()
            return parsed
        except Exception as e:
            last_err = e
            if _note_llm_error(e):
                break                       # billing error — won't recover mid-run
            if attempt < attempts - 1:
                time.sleep(2)
    # After retries the LLM still didn't return valid JSON. Return a sentinel so
    # the caller can retry next run instead of silently marking the job seen.
    _llm_note_failure()
    log.warning(f"LLM eval failed for LinkedIn job {job['id']} after {attempts} tries: {last_err}")
    return {"_eval_failed": True}


# ─── STATE ───────────────────────────────────────────────────────────────────
def load_state() -> dict:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    return json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}

def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))

# Pending alerts are persisted alongside state so an interrupted run (machine
# sleep, kill) neither loses queued alerts nor re-loses jobs it already marked
# seen. save_state() and save_pending() are always called together to stay
# consistent; the file is cleared only after the digest email is sent.
def load_pending() -> list:
    try:
        return json.loads(PENDING_FILE.read_text()) if PENDING_FILE.exists() else []
    except Exception:
        return []

def save_pending(pending: list):
    try:
        PENDING_FILE.write_text(json.dumps(pending, ensure_ascii=False, indent=2))
    except Exception as e:
        log.warning(f"save_pending failed: {e}")

def clear_pending():
    try:
        PENDING_FILE.unlink(missing_ok=True)
    except Exception:
        pass

def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


# ─── LLM EVALUATION (careers page changes) ───────────────────────────────────
SENIOR_TITLE_RE = re.compile(
    r"\b("
    r"director|vp|vice president|head of|principal|chief|president|executive|"
    r"managing director|partner|"
    r"lead|"
    r"senior|sr\.?\s|"
    r"manager"
    r")\b",
    re.IGNORECASE,
)

# Catch JDs whose MINIMUM required experience is 3 years or more, e.g.
# "3+ years", "minimum 3 years", "3-5 years", "5+ years of experience". The
# candidate is a new grad: a floor of 3+ years means the role wants an
# experienced hire, so it's disqualifying. Roles with a lower bound of 0-2
# (incl. "1-3 years", "2-4 years", "up to 3 years") stay in.
# Lookbehinds keep the LOWER bound governing a range: (?<![-–\d]) skips the upper
# bound of "1-3 years" (min 1 → keep); (?<!to ) skips "2 to 4" / "up to 3 years".
_EXP_YEARS_RE = re.compile(
    r"(?<![-–\d])(?<!to )\b(?:[3-9]|\d{2,})\s*(?:[-–]\s*\d+|\s+to\s+\d+|\+)?\s*years?\s+of(?:\s+\w+){0,3}\s+experience",
    re.IGNORECASE,
)

# The lookbehind above only guards the NO-SPACE range form ("1-3 years"). Spaced
# ranges ("0 – 3 years", "1 - 3 years") put a space right before the upper number,
# so the lookbehind passes and the range's UPPER bound is misread as a floor —
# silently dropping the most on-target jobs (Mackenzie "0 – 3 years" was killed
# this way). Collapse the whitespace around a digit–digit dash first so the guard
# governs. Always use this instead of _EXP_YEARS_RE.search() directly.
_RANGE_WS_RE = re.compile(r"(\d)\s*[-–]\s*(\d)")

def exp_floor_ge3(text: str) -> bool:
    """True iff the JD's MINIMUM required experience is 3+ years (disqualifying).

    Normalizes spaced numeric ranges ('0 – 3 years' -> '0-3 years') so a range's
    upper bound isn't mistaken for a floor. '3+ years'/'5+ years'/'3-5 years'
    (floor >=3) still match; '0-3'/'1-3'/'up to 3 years' (floor 0-2) do not."""
    return bool(_EXP_YEARS_RE.search(_RANGE_WS_RE.sub(r"\1-\2", text or "")))

_TRAILING_DATE_RE = re.compile(r"\s*\|\s*\d{4}-\d{2}-\d{2}\s*$")
# Structured-feed marker: [src:id] prefix carrying an immutable job ID.
# src is the source adapter (gh=Greenhouse, js=Jane Street, …); id is stable.
_ID_RE = re.compile(r"^\[([a-z]+):([\w-]+)\]\s*")

def extract_jobs(text: str) -> dict[str, str]:
    """Map each candidate job line to a stable signature.

    Returns {signature: display_line}.

    Structured feeds (Greenhouse, Jane Street, …) prefix each line with
    [src:id] carrying an immutable job ID, so EVERY posting is kept — no
    length or seniority pre-filter — and keyed by that ID (robust to
    title/location rewording). Seniority/experience filtering happens later
    in the LLM step; we never drop a posting here, so a mis-titled entry
    role can't be silently lost. Priority is recall: don't miss.

    Generic HTML has no per-job structure, so BeautifulSoup's get_text()
    returns nav/heading/footer noise mixed with titles. There we apply a
    minimal length + obvious-senior filter purely to keep that noise out of
    the signature set; the fallback signature is the title text line itself.
    """
    jobs = {}
    for raw in text.splitlines():
        s = _TRAILING_DATE_RE.sub("", raw.strip())
        if not s:
            continue
        m = _ID_RE.match(s)
        if m:
            jobs[f"{m.group(1)}:{m.group(2)}"] = _ID_RE.sub("", s)  # keep ALL feed jobs
        elif len(s) > 15 and not SENIOR_TITLE_RE.search(s):
            jobs[s] = s                                             # HTML: filter noise
    return jobs


def evaluate(company: str, added_lines: list[str], jd_ctx: dict[str, str] | None = None) -> dict | None:
    if not DEEPSEEK_KEY or not added_lines:
        return None
    # Evaluate in chunks so EVERY posting is actually seen by the LLM — never
    # truncated away and then silently marked seen. If any chunk can't be
    # evaluated (LLM fails after retries) return None, so the whole batch is
    # held for retry next run (recall-first: never drop an unseen posting).
    CHUNK = 20
    any_relevant, roles, reasons, matched_lines = False, [], [], []
    for i in range(0, len(added_lines), CHUNK):
        chunk = added_lines[i:i + CHUNK]
        # Number each posting so the LLM can point back at the EXACT line it found
        # relevant (via relevant_indices). We then map that line → its per-job
        # link, instead of fuzzy-matching the LLM's paraphrased role text against
        # job titles (which failed on TalentBrew's long descriptive titles and
        # left alerts with only the API-endpoint fallback URL).
        numbered = [f"[{n}] {ln}" for n, ln in enumerate(chunk)]
        content = "\n".join(numbered)
        # Append per-role JD excerpts when available so the experience/seniority
        # checks have real text to judge (title alone hides the years bar).
        if jd_ctx:
            blocks = [f"### [{n}] {ln}\n{jd_ctx[ln][:1200]}"
                      for n, ln in enumerate(chunk) if jd_ctx.get(ln)]
            if blocks:
                content += "\n\nJob description excerpts:\n" + "\n\n".join(blocks)
        res = _eval_llm(company, content)
        if res is None:
            return None
        if res.get("relevant"):
            any_relevant = True
            roles.extend(res.get("roles", []))
            for idx in res.get("relevant_indices", []):
                if isinstance(idx, int) and 0 <= idx < len(chunk):
                    matched_lines.append(chunk[idx])
            if res.get("reason"):
                reasons.append(res["reason"])
    return {"relevant": any_relevant,
            "roles": list(dict.fromkeys(roles)),
            "matched_lines": list(dict.fromkeys(matched_lines)),
            "reason": "; ".join(reasons)[:300]}


def _eval_llm(company: str, content: str) -> dict | None:
    """One relevance call to DeepSeek with retries. Returns parsed dict or None."""
    client = OpenAI(api_key=DEEPSEEK_KEY, base_url="https://api.deepseek.com")
    prompt = f"""A company's careers page changed. Evaluate if the new content contains job postings relevant to this candidate.

Company: {company}

New / changed content on the page (each posting is prefixed with an index like [0], [1], …):
{content}

Candidate profile:
{USER_PROFILE}

Before answering, apply these checks in order. If ANY of them fail, set relevant=false
regardless of how well the function area matches — this candidate graduates December 2026
and can apply to new-grad/entry-level roles AND junior roles whose minimum requirement is
under 3 years (0-2 years) — but NOT roles that require a minimum of 3 or more years.

1. Title check: does the title contain a senior/management seniority word (Director, VP,
   Vice President, Head of, Principal, Chief, President, Executive, Managing Director,
   Partner)? -> not relevant. ("Senior"/"Sr."/"Lead"/"Manager" alone is NOT an automatic
   reject — decide by the years requirement in check 2.)
2. Experience check — judge by the UPPER bound of the required experience.
   -> NOT relevant if EITHER: (a) a stated range whose UPPER bound is 4 or more years
   ("2-5 years", "2-4 years", "3-7 years", "1-5 years"); OR (b) a minimum-only figure whose
   floor is 3 or more ("3+ years", "minimum 3 years", "at least 3 years", "5+ years").
   -> KEEP if ANY: a range whose upper bound is 3 or less ("0-2", "1-3", "0-3", "up to 3 years");
   OR a minimum-only figure with floor 0-2 and NO upper bound ("1+ years", "2+ years", vague
   "prior experience"/"background in" with no number); OR new grad / no experience stated.
3. Internship check: is this an internship, co-op, or temporary/seasonal program rather
   than a full-time permanent role? -> not relevant.
4. Location check: not in US, UK/London, Hong Kong, Singapore, or Netherlands (remote-only also ok)? -> not relevant. (Australia/New Zealand are NOT targeted — treat AU/NZ locations as not relevant.)
5. Keep relevant=true ONLY if the role is a quantitative-research / quantitative-or-systematic-
   trading / quantitative-developer / quantitative-analyst / index (analyst/researcher) /
   portfolio-management function AND is genuine full-time at most 3 years (new-grad, entry-
   level, Analyst, Associate, or junior ≤3yr). ANY software-engineering role is NOT relevant —
   "Software Engineer", "Software Developer", Java/Python/C++/C#/full-stack/backend/frontend/
   web/DevOps/platform — EVEN "for trading systems" or at a quant firm. ("Quantitative
   Developer" IS wanted; "Software Engineer" is not.)

Reply ONLY in this JSON format (no other text):
{{
  "relevant": true or false,
  "reason": "one sentence",
  "roles": ["role name 1", "role name 2"],
  "relevant_indices": [0, 3]
}}
"relevant_indices" MUST list the [index] numbers of exactly the postings you judged
relevant (same count/order as "roles"). Use the numbers shown in the content; do not
invent numbers. If relevant is false, use an empty list."""

    last_err = None
    attempts = _llm_attempts()
    for attempt in range(attempts):
        try:
            msg = client.chat.completions.create(
                model="deepseek-v4-flash",
                max_tokens=8192,   # reasoner burns tokens on reasoning first; too low → empty content
                messages=[{"role": "user", "content": prompt}],
            )
            text = msg.choices[0].message.content.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
                if text.startswith("json"):
                    text = text[4:].strip()
            parsed = json.loads(text)
            _llm_note_success()
            return parsed
        except Exception as e:
            last_err = e
            if _note_llm_error(e):
                break                       # billing error — won't recover mid-run
            if attempt < attempts - 1:
                time.sleep(2)
    _llm_note_failure()
    log.warning(f"LLM eval failed for {company} after {attempts} tries: {last_err}")
    return None


# ─── NOTIFICATIONS ────────────────────────────────────────────────────────────
def notify_macos(title: str, body: str):
    notifier = shutil.which("terminal-notifier")
    if notifier:
        subprocess.run(
            [notifier, "-title", title, "-message", body, "-sound", "Glass"],
            capture_output=True,
        )
    else:
        safe_title = title.replace('"', "'")
        safe_body  = body.replace('"', "'")
        subprocess.run(
            ["osascript", "-e", f'display notification "{safe_body}" with title "{safe_title}" sound name "Glass"'],
            capture_output=True,
        )

def notify_email(subject: str, body: str) -> bool:
    """Send an email. Returns True only on a confirmed successful send."""
    if not RESEND_API_KEY:
        log.warning("RESEND_API_KEY not set, skipping email")
        return False
    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
            json={
                "from": "Job Monitor <onboarding@resend.dev>",
                "to": [EMAIL_TO],
                "subject": subject,
                "text": body,
            },
            timeout=30,
        )
        if resp.status_code >= 400:
            log.warning(f"Email failed: {resp.status_code} {resp.text[:200]}")
            return False
        log.info(f"Email sent: {subject}")
        return True
    except Exception as e:
        log.warning(f"Email failed: {e}")
        return False

def alert_macos(company: str, roles: list[str]):
    """Immediate macOS notification for each new posting."""
    roles_str = ", ".join(roles) if roles else "new posting"
    notify_macos(f"Job alert: {company}", roles_str)
    log.info(f"Alert queued: {company} | {roles_str}")


def verify_url(url: str) -> bool:
    """Return True if the URL responds with a non-error HTTP status."""
    try:
        resp = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
        if resp.status_code == 405:  # HEAD not allowed, try GET
            resp = requests.get(url, headers=HEADERS, timeout=10, stream=True)
            resp.close()
        return resp.status_code < 400
    except Exception:
        return False


_US_KW      = {"new york", "nyc", "chicago", "san francisco", "boston", "stamford", "united states", ", ny", ", il", ", ca", ", ma", ", ct"}
_LONDON_KW  = {"london", "united kingdom", "uk"}
_HK_KW      = {"hong kong", "hk"}
_SG_KW      = {"singapore", "sg"}
_NL_KW      = {"netherlands", "amsterdam"}

def _location_rank(location: str) -> int:
    loc = location.lower()
    if any(kw in loc for kw in _US_KW):     return 0
    if any(kw in loc for kw in _LONDON_KW): return 1
    if any(kw in loc for kw in _HK_KW):     return 2
    if any(kw in loc for kw in _SG_KW):     return 3
    if any(kw in loc for kw in _NL_KW):     return 4
    return 5  # unknown / other (Australia/NZ no longer targeted)

def load_watchlist() -> list:
    """Jobs the user is still deciding on — surfaced in EVERY daily email (even a
    'no new jobs' one) until they say they've decided. Stored in state/watchlist.json
    as [{company, title, url, added}]."""
    wf = BASE_DIR / "state" / "watchlist.json"
    try:
        return json.loads(wf.read_text()) if wf.exists() else []
    except Exception:
        return []


def _watchlist_block() -> str:
    wl = load_watchlist()
    if not wl:
        return ""
    lines = ["", "═" * 48,
             f"⏰ 你让我持续提醒、还在纠结的岗位({len(wl)} 个,决定后跟我说一声就撤下):"]
    for w in wl:
        lines.append(f"  • {w.get('company','')} — {w.get('title','')}")
        if w.get("url"):
            lines.append(f"    {w['url']}")
        if w.get("added"):
            lines.append(f"    (加入提醒:{w['added']})")
    return "\n".join(lines)


def send_daily_summary(found: list[dict], companies_checked: int):
    """Send one daily digest email — always, even when nothing found."""
    # De-dup (a resumed run may re-queue an alert already in the persisted file)
    seen_keys, deduped = set(), []
    for it in found:
        key = (it.get("company", ""), it.get("roles", ""), it.get("url", ""))
        if key not in seen_keys:
            seen_keys.add(key)
            deduped.append(it)
    found = deduped
    # Sort: 官网(careers)岗位优先 — they carry a real single-job apply link and
    # are higher-signal than LinkedIn, so they lead the digest (previously they
    # have no `location` field and got dumped to the very bottom as "other").
    # Within each source, order by location priority: US > London > HK > SG > NL > other.
    found = sorted(found, key=lambda x: (
        1 if x.get("source", "careers") == "linkedin" else 0,
        _location_rank(x.get("location", "")),
    ))
    today = datetime.now().strftime("%Y-%m-%d")
    if not found:
        subject = f"[Job Monitor] {today} — 今日无新岗位"
        body = (
            f"扫描完成 — {today}\n\n"
            f"共检查 {companies_checked} 家公司 + LinkedIn，今日无新相关岗位。\n\n"
            f"— Job Monitor"
        )
    else:
        subject = f"[Job Monitor] {today} — 发现 {len(found)} 个新岗位"
        lines = [f"扫描完成 — {today}，发现 {len(found)} 个相关岗位：\n"]
        for i, item in enumerate(found, 1):
            lines.append(f"{'─' * 48}")
            source = item.get("source", "careers")
            lines.append(f"{i}. {item['company']}  {'[LinkedIn]' if source == 'linkedin' else '[官网]'}")
            lines.append(f"   职位：{item['roles']}")
            location = item.get("location", "")
            if location:
                lines.append(f"   地点：{location}")
            lines.append(f"   说明：{item['reason']}")
            job_links = item.get("job_links") or []
            if job_links:
                for jl in job_links:
                    lines.append(f"   链接：{jl}")
            else:
                url = item.get("url", "")
                if url:
                    lines.append(f"   链接：{url}")
        lines.append(f"\n{'─' * 48}")
        lines.append(f"\n— Job Monitor  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        body = "\n".join(lines)
    body += _watchlist_block()          # 待决策岗位:每日提醒,直到用户说已决定
    ok = notify_email(subject, body)
    if ok:
        log.info(f"Daily summary sent: {len(found)} alert(s)")
    else:
        log.warning(f"Daily summary NOT delivered ({len(found)} alert(s)) — "
                    f"will retry on next run")
    return ok


# ─── MAIN ────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("Job monitor started")

    if not wait_for_network():
        log.error("No network after 60 minutes — aborting")
        notify_macos("Job Monitor Error", "No internet connection after 60 min wait.")
        return

    companies = load_companies()
    if not companies:
        log.error("No companies loaded — check Notion token / companies.json")
        notify_macos("Job Monitor Error", "No companies loaded (Notion token or companies.json).")
        return
    log.info(f"Loaded {len(companies)} companies")

    state   = load_state()
    pending = load_pending()   # resume alerts queued by a prior interrupted run
    if pending:
        log.info(f"Resumed {len(pending)} pending alert(s) from an interrupted run")
    now     = datetime.now()

    # ── Careers page monitoring ───────────────────────────────────────────────
    for i, company in enumerate(companies):
        name, url, page_id = company["name"], company["url"], company["id"]
        log.info(f"[{i+1}/{len(companies)}] Checking: {name}")

        new_text = fetch_text(url)
        if not new_text:
            continue

        new_hash = content_hash(new_text)
        current  = extract_jobs(new_text)          # {signature: display_line}
        # Map harvested page anchors onto text-signature jobs (generic HTML/render
        # companies with no structured adapter) so their alerts carry a real job
        # link instead of the company search page. Structured feeds already set
        # _JOB_URL via their adapter and are skipped.
        if _PAGE_LINKS:
            for _sig, _disp in current.items():
                if _sig in _JOB_URL:
                    continue
                _nt = _norm_title(_disp)
                if not _nt:
                    continue
                # Exact match only: linking a role to the WRONG posting is worse
                # than a search-page fallback, so we never fuzzy-guess. Sites whose
                # anchor text is the clean title (Optiver/Macquarie/ING/…) resolve;
                # TalentBrew-style "id date title location" anchors won't match and
                # safely fall back to the company search URL.
                _href = _PAGE_LINKS.get(_nt)
                if _href:
                    _JOB_URL[_sig] = _href
        rec      = state.get(name, {})
        seen     = set(rec.get("seen_jobs", []))
        unevaluated: set[str] = set()   # new jobs whose LLM eval failed — hold for retry

        if "seen_jobs" not in rec:
            # First time under the seen-jobs scheme (or a brand-new company):
            # seed the current listings as baseline silently — no back-fill.
            log.info(f"  → Baseline seeded ({len(current)} jobs)")
        else:
            new_sigs = set(current) - seen
            if new_sigs:
                # When JD text is available (structured feeds), drop roles whose
                # JD explicitly requires ≥2 yrs — same hard filter as LinkedIn.
                # Title-only jobs (no JD) are kept (recall-first).
                kept = [s for s in sorted(new_sigs)
                        if not exp_floor_ge3(_JD_TEXT.get(s, ""))]
                dropped = len(new_sigs) - len(kept)
                log.info(f"  → {len(new_sigs)} new job(s)"
                         + (f" ({dropped} dropped: ≥3y experience)" if dropped else ""))
                if kept:
                    new_display = [current[s] for s in kept]
                    jd_ctx = {current[s]: _JD_TEXT[s] for s in kept if _JD_TEXT.get(s)}
                    result = evaluate(name, new_display, jd_ctx)
                    if result is None:
                        # LLM eval failed (after retries) — do NOT mark these seen,
                        # so they are re-evaluated next run instead of lost silently.
                        unevaluated = set(kept)
                        log.warning(f"  → LLM eval failed; holding {len(kept)} job(s) for retry next run")
                else:
                    result = None
                if result and result.get("relevant"):
                    roles = result.get("roles", [])
                    alert_macos(name, roles)
                    # Attach the per-job link of exactly the postings the LLM flagged
                    # relevant (by index → display line → signature → _JOB_URL). This
                    # is exact, not a fuzzy title guess, so it never mislinks to an
                    # unrelated posting. When a matched line has no _JOB_URL, the
                    # email falls back to human_url(url) — a real, browser-openable
                    # careers/search page, never a raw API endpoint (which rendered
                    # as JSON/乱码 before).
                    disp_to_sig = {current[s]: s for s in kept}
                    job_links = []
                    for ml in result.get("matched_lines", []):
                        s = disp_to_sig.get(ml)
                        if s and _JOB_URL.get(s):
                            job_links.append(_JOB_URL[s])
                    job_links = list(dict.fromkeys(job_links))[:10]
                    pending.append({"company": name, "roles": ", ".join(roles),
                                    "reason": result.get("reason", ""),
                                    "url": human_url(url),
                                    "job_links": job_links, "source": "careers"})

        state[name] = {
            **rec,
            "hash":           new_hash,
            "seen_jobs":      sorted((seen | set(current)) - unevaluated),
            "last_checked":   now.isoformat(),
            "url":            url,
            "notion_page_id": page_id,
        }
        # Drop the old full-text snapshot; seen_jobs replaces it
        state[name].pop("text", None)
        # Persist progress periodically so an interrupted run keeps what it did
        # (seen + queued alerts stay consistent — saved together).
        if (i + 1) % 10 == 0:
            save_state(state)
            save_pending(pending)
        time.sleep(0.5)

    close_browser()   # release headless Chromium after careers monitoring
    save_state(state)
    save_pending(pending)

    # ── LinkedIn job search ───────────────────────────────────────────────────
    log.info("Starting LinkedIn job search")
    li_state  = state.setdefault("_linkedin_seen", {})
    li_alerts = 0

    # Build lowercase name → (page_id, url) lookup for Notion matching
    notion_lookup = {c["name"].lower(): c for c in companies}

    for keyword in LINKEDIN_KEYWORDS:
        for location in LINKEDIN_LOCATIONS:
            log.info(f"  LinkedIn search: '{keyword}' @ {location}")
            jobs = search_linkedin_jobs(keyword, location)
            log.info(f"    → {len(jobs)} listings found")
            for job in jobs:
                job_id = job["id"]
                if job_id in li_state:
                    continue
                result = evaluate_linkedin_job(job)
                if result and result.get("_eval_failed"):
                    # Transient LLM failure — do NOT record in li_state, so this
                    # job is re-evaluated next run instead of being lost silently.
                    continue
                if result and result.get("relevant"):
                    roles = result.get("roles", [job["title"]])
                    # The job came from LinkedIn's own search feed, so its posting
                    # id is canonical and the view URL is valid by construction.
                    # Do NOT gate it on verify_url: LinkedIn throttles rapid bot
                    # requests with 403/429/999, so a failure there is a false
                    # negative that would blank a perfectly good link (this dropped
                    # e.g. the SocGen Junior Trader link on 2026-08-06). Prefer the
                    # clean numeric /jobs/view/{id} form, which always resolves.
                    job_url = (f"https://www.linkedin.com/jobs/view/{job_id}"
                               if str(job_id).isdigit() else job["url"])
                    alert_macos(job["company"], roles)
                    li_alerts += 1
                    pending.append({
                        "company":  job["company"],
                        "roles":    ", ".join(roles),
                        "reason":   result.get("reason", ""),
                        "url":      job_url,
                        "source":   "linkedin",
                        "location": job.get("location", ""),
                    })
                li_state[job_id] = {
                    "title":    job["title"],
                    "company":  job["company"],
                    "url":      job["url"],
                    "seen_at":  now.isoformat(),
                    "relevant": result.get("relevant", False) if result else False,
                }
            save_state(state)      # li_state grows in-place; persist per search
            save_pending(pending)
            time.sleep(1)

    save_state(state)
    save_pending(pending)

    # ── One daily digest email — at most once per calendar day ──────────────
    # A day can see several runs (scheduled launchd run + manual re-run + a
    # resumed run after a crash). Only the first successful send goes out; later
    # runs still scan and update state, but their finds stay in pending.json and
    # roll into tomorrow's digest — so we never spam, and never miss.
    today = datetime.now().strftime("%Y-%m-%d")
    if _LLM_BALANCE_ERR:
        # Billing failure: every job went unevaluated (held for retry, not marked
        # seen). Do NOT send a misleading "no new jobs" digest or set
        # last_email_date — surface the real problem and let a re-run (after a
        # top-up) produce the true digest today.
        msg = ("DeepSeek API 余额不足(402 Insufficient Balance),今天所有岗位都没能评估。"
               "岗位已暂存待重评——请充值后重跑,不会漏。")
        log.error(msg)
        notify_email("[Job Monitor] ⚠️ DeepSeek 余额不足 — 请充值", msg)
        notify_macos("Job Monitor 余额不足", "DeepSeek 余额用尽,请充值后重跑")
    elif state.get("last_email_date") == today:
        log.info(f"Email already sent today ({today}); holding {len(pending)} "
                 f"alert(s) for the next digest.")
        # keep pending.json — these carry into the next day's email
    elif send_daily_summary(pending, len(companies)):
        state["last_email_date"] = today
        save_state(state)
        clear_pending()   # digest sent — safe to drop the persisted queue
    else:
        # Delivery failed (e.g. network down). Do NOT mark today as emailed and
        # do NOT clear pending — the queued alerts persist and roll into the next
        # run's digest, so a failed send never silently loses jobs.
        log.warning("Email not delivered — keeping pending.json for next run's digest")
    log.info(f"Done — {len(pending)} alert(s) ({li_alerts} from LinkedIn). {len(companies)} companies checked.")


if __name__ == "__main__":
    main()
