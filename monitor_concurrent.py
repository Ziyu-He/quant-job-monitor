"""GitHub-bound CONCURRENT variant of monitor.py.

Identical logic to monitor.py, but the slow DeepSeek evaluations run in a thread
pool instead of one at a time — the run's wall-clock is dominated by evaluation
latency, so this is where the ~2h shrinks.

This file does NOT modify monitor.py. It imports every helper from it and only
re-expresses the two main loops (careers + LinkedIn) as three explicit stages:

    collect  — SERIAL fetch + extract, snapshotting each company's jd_ctx and
               per-job links into a payload (the module globals _JOB_URL/_JD_TEXT
               are cleared on every fetch, so they MUST be snapshotted now).
    eval     — evaluate the payloads; concurrent=True runs them in a thread pool,
               concurrent=False one at a time. This is the ONLY stage that differs
               between the serial and concurrent paths.
    apply    — SERIAL, on the main thread: append pending + update state. No shared
               mutable state is written concurrently, so no locks are needed.

Because serial and concurrent share the exact same collect and apply code and
differ only in the eval scheduling flag, the equivalence test only has to pin
down that eval scheduling doesn't change the result set. See test_equivalence.py.

DeepSeek eval (`monitor.evaluate` / `monitor.evaluate_linkedin_job`) is called
through the `monitor.` module reference on purpose, so a test can monkeypatch it.
"""
import sys
import time
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import monitor
from monitor import (
    fetch_text, extract_jobs, content_hash, human_url, _norm_title,
    load_state, save_state, load_pending, save_pending, clear_pending,
    alert_macos, send_daily_summary, notify_email, notify_macos, close_browser,
    load_companies,
    LINKEDIN_KEYWORDS, LINKEDIN_LOCATIONS, _EXP_YEARS_RE,
)

log = monitor.log
EVAL_WORKERS = 12   # concurrent DeepSeek evaluations (careers + LinkedIn)

# LinkedIn gentle-search tuning. Today's local run got the IP rate-limited by
# deep-paginating 70 searches too fast (45 pages each), then SILENTLY DROPPED the
# rest (70 searches degraded to 2 results). Instead of giving up on a throttled
# page, we back off and retry, and re-run any search that stayed throttled after
# a cooldown at the end of the round — so a transient 429 never means missed jobs.
_LI_PAGES = monitor._LI_PAGES
_LI_STEP = monitor._LI_PAGE_STEP
_LI_BACKOFF = [2, 5, 10]          # exponential back-off (s) across 3 retries
_LI_THROTTLE_COOLDOWN = 90        # cool-down (s) before re-running throttled searches
_LI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.linkedin.com/",
}


def _li_search_url(kw, loc, page):
    return ("https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
            f"?keywords={urllib.parse.quote(kw)}"
            + (f"&location={urllib.parse.quote(loc)}" if loc else "")
            + "&f_TPR=r129600"          # last 36 hours
            + f"&start={page * _LI_STEP}")


def _li_parse_cards(html):
    """Parse job cards from a guest-API page (same fields as monitor.py)."""
    out = []
    for card in BeautifulSoup(html, "html.parser").find_all("div", class_=re.compile(r"base-card")):
        title_el = card.find(class_=re.compile(r"base-search-card__title"))
        comp_el  = card.find(class_=re.compile(r"base-search-card__subtitle"))
        loc_el   = card.find(class_=re.compile(r"job-search-card__location"))
        link_el  = card.find("a", href=re.compile(r"linkedin\.com/jobs/view"))
        time_el  = card.find("time")
        if not (title_el and comp_el and link_el):
            continue
        if time_el and "repost" in (time_el.get("class") or []):
            continue
        if card.find(string=re.compile(r"repost", re.I)):
            continue
        href = link_el.get("href", "")
        m = re.search(r"-(\d{7,12})(?:\?|$)", href)
        out.append({"id": m.group(1) if m else href,
                    "title": title_el.get_text(strip=True),
                    "company": comp_el.get_text(strip=True),
                    "location": loc_el.get_text(strip=True) if loc_el else "",
                    "url": href.split("?")[0],
                    "posted": time_el.get("datetime", "") if time_el else ""})
    return out


def search_linkedin_gentle(keyword, location=""):
    """Deep-paginate one keyword@location with patient 429 back-off/retry.
    Returns (jobs, throttled): throttled=True means a page stayed non-200 after
    all retries and pagination stopped early (search should be re-run later)."""
    jobs, seen, throttled = [], set(), False
    try:
        for page in range(_LI_PAGES):
            url = _li_search_url(keyword, location, page)
            resp = None
            for attempt in range(4):     # 1 try + 3 back-off retries
                if attempt:
                    time.sleep(_LI_BACKOFF[min(attempt - 1, 2)])
                elif page:
                    time.sleep(0.8)      # gentle first-touch pacing per page
                try:
                    resp = requests.get(url, headers=_LI_HEADERS, timeout=20)
                except Exception:
                    resp = None
                    continue
                if resp.status_code == 200:
                    break
            if resp is None or resp.status_code != 200:
                log.warning(f"LinkedIn throttled {resp.status_code if resp else 'err'} "
                            f"for '{keyword}' @ {location} at page {page} after retries; "
                            f"will re-run this search after cooldown")
                throttled = True
                break
            page_jobs = _li_parse_cards(resp.text)
            if not page_jobs:
                break
            new = 0
            for j in page_jobs:
                if j["id"] in seen:
                    continue
                seen.add(j["id"]); new += 1; jobs.append(j)
            if new == 0:
                break
        return jobs, throttled
    except Exception as e:
        log.warning(f"LinkedIn search error for '{keyword}': {e}")
        return jobs, throttled


# ─────────────────────────────  CAREERS  ─────────────────────────────

def collect_careers_payloads(companies, state, now):
    """SERIAL fetch. Returns (payloads, direct_state).

    payloads     — companies with new jobs to evaluate (each carries a full
                   snapshot: kept sigs, new_display, jd_ctx, disp_to_sig,
                   job_url_snap, and the base state dict).
    direct_state — {name: state_dict} for companies needing NO eval (baseline
                   seed, no new jobs, or all-dropped) — their seen_jobs are final.
    """
    payloads, direct_state = [], {}
    for i, company in enumerate(companies):
        name, url, page_id = company["name"], company["url"], company["id"]
        log.info(f"[{i+1}/{len(companies)}] Fetching: {name}")
        new_text = fetch_text(url)
        if not new_text:
            continue

        new_hash = content_hash(new_text)
        current = extract_jobs(new_text)   # {signature: display_line}
        # Map harvested page anchors onto text-signature jobs so their alerts
        # carry a real per-job link (identical to monitor.py's inline block).
        if monitor._PAGE_LINKS:
            for _sig, _disp in current.items():
                if _sig in monitor._JOB_URL:
                    continue
                _nt = _norm_title(_disp)
                if not _nt:
                    continue
                _href = monitor._PAGE_LINKS.get(_nt)
                if _href:
                    monitor._JOB_URL[_sig] = _href

        rec = state.get(name, {})
        seen = set(rec.get("seen_jobs", []))
        base = {**rec, "hash": new_hash, "last_checked": now.isoformat(),
                "url": url, "notion_page_id": page_id}
        base.pop("text", None)

        if "seen_jobs" not in rec:
            # First time under seen-jobs scheme — seed silently, no back-fill.
            log.info(f"  → Baseline seeded ({len(current)} jobs)")
            base["seen_jobs"] = sorted(set(current))
            direct_state[name] = base
        else:
            new_sigs = set(current) - seen
            kept = [s for s in sorted(new_sigs)
                    if not _EXP_YEARS_RE.search(monitor._JD_TEXT.get(s, ""))]
            if new_sigs:
                dropped = len(new_sigs) - len(kept)
                log.info(f"  → {len(new_sigs)} new job(s)"
                         + (f" ({dropped} dropped: ≥3y experience)" if dropped else ""))
            if kept:
                new_display = [current[s] for s in kept]
                jd_ctx = {current[s]: monitor._JD_TEXT[s]
                          for s in kept if monitor._JD_TEXT.get(s)}
                disp_to_sig = {current[s]: s for s in kept}
                job_url_snap = {s: monitor._JOB_URL[s]
                                for s in kept if monitor._JOB_URL.get(s)}
                payloads.append({
                    "name": name, "url": url, "seen": seen,
                    "current_sigs": set(current), "kept": kept,
                    "new_display": new_display, "jd_ctx": jd_ctx,
                    "disp_to_sig": disp_to_sig, "job_url_snap": job_url_snap,
                    "base": base,
                })
            else:
                # No new jobs (or all dropped by the ≥3y filter): seen is final.
                base["seen_jobs"] = sorted(seen | set(current))
                direct_state[name] = base

        time.sleep(0.5)   # pacing between fetches, same as monitor.py
    return payloads, direct_state


def eval_careers_payloads(payloads, concurrent):
    """Evaluate each payload. The ONLY stage that differs serial vs concurrent."""
    def run(p):
        return p, monitor.evaluate(p["name"], p["new_display"], p["jd_ctx"])
    if concurrent:
        with ThreadPoolExecutor(max_workers=EVAL_WORKERS) as ex:
            return list(ex.map(run, payloads))
    return [run(p) for p in payloads]


def apply_careers_results(results, direct_state, state, pending):
    """SERIAL, main thread: write state + append pending. Order-independent."""
    for name, base in direct_state.items():
        state[name] = base
    for p, result in results:
        name = p["name"]
        unevaluated = set()
        if result is None:
            # LLM eval failed after retries — hold these for retry (do NOT mark
            # seen), exactly like monitor.py.
            unevaluated = set(p["kept"])
            log.warning(f"  → LLM eval failed for {name}; "
                        f"holding {len(p['kept'])} job(s) for retry next run")
        if result and result.get("relevant"):
            roles = result.get("roles", [])
            alert_macos(name, roles)
            job_links = []
            for ml in result.get("matched_lines", []):
                s = p["disp_to_sig"].get(ml)
                if s and p["job_url_snap"].get(s):
                    job_links.append(p["job_url_snap"][s])
            job_links = list(dict.fromkeys(job_links))[:10]
            pending.append({"company": name, "roles": ", ".join(roles),
                            "reason": result.get("reason", ""),
                            "url": human_url(p["url"]),
                            "job_links": job_links, "source": "careers"})
        base = dict(p["base"])
        base["seen_jobs"] = sorted((p["seen"] | p["current_sigs"]) - unevaluated)
        state[name] = base


# ─────────────────────────────  LINKEDIN  ────────────────────────────

def collect_linkedin_jobs(state):
    """SERIAL gentle search across keyword×location. Returns the list of NEW jobs
    to evaluate (deduped by job id; excludes ids already frozen in _linkedin_seen).

    Searches that stay throttled after retries are re-run once after a cooldown,
    so a transient 429 never silently drops a whole keyword@location (today's bug).
    """
    li_state = state.setdefault("_linkedin_seen", {})
    collected, seen_ids, throttled_pairs = [], set(), []

    def harvest(keyword, location):
        jobs, throttled = search_linkedin_gentle(keyword, location)
        n_new = 0
        for job in jobs:
            jid = job["id"]
            if jid in li_state or jid in seen_ids:
                continue
            seen_ids.add(jid)
            collected.append(job)
            n_new += 1
        return len(jobs), n_new, throttled

    for keyword in LINKEDIN_KEYWORDS:
        for location in LINKEDIN_LOCATIONS:
            log.info(f"  LinkedIn search: '{keyword}' @ {location}")
            total, n_new, throttled = harvest(keyword, location)
            log.info(f"    → {total} listings, {n_new} new"
                     + (" [THROTTLED — will re-run]" if throttled else ""))
            if throttled:
                throttled_pairs.append((keyword, location))
            time.sleep(1)

    # End-of-round recovery: re-run the searches that stayed throttled, after a
    # cooldown to let the rate-limit window pass. This is what today's local run
    # lacked — it dropped throttled searches and missed their jobs.
    if throttled_pairs:
        log.warning(f"{len(throttled_pairs)} search(es) throttled; cooling down "
                    f"{_LI_THROTTLE_COOLDOWN}s then re-running them")
        time.sleep(_LI_THROTTLE_COOLDOWN)
        still = []
        for keyword, location in throttled_pairs:
            log.info(f"  LinkedIn RE-RUN: '{keyword}' @ {location}")
            total, n_new, throttled = harvest(keyword, location)
            log.info(f"    → {total} listings, {n_new} new"
                     + (" [STILL THROTTLED]" if throttled else ""))
            if throttled:
                still.append((keyword, location))
            time.sleep(2)
        if still:
            log.warning(f"STILL throttled after re-run — genuinely missed: {still}")
    return collected


def eval_linkedin_jobs(jobs, concurrent):
    """Evaluate each LinkedIn job. Only stage that differs serial vs concurrent."""
    def run(job):
        return job, monitor.evaluate_linkedin_job(job)
    if concurrent:
        with ThreadPoolExecutor(max_workers=EVAL_WORKERS) as ex:
            return list(ex.map(run, jobs))
    return [run(job) for job in jobs]


def apply_linkedin_results(results, state, pending, now):
    """SERIAL, main thread. Returns count of LinkedIn alerts queued."""
    li_state = state.setdefault("_linkedin_seen", {})
    li_alerts = 0
    for job, result in results:
        jid = job["id"]
        if result and result.get("_eval_failed"):
            # Transient LLM failure — do NOT record in li_state, so it is
            # re-evaluated next run instead of being lost. (Same as monitor.py.)
            continue
        if result and result.get("relevant"):
            roles = result.get("roles", [job["title"]])
            job_url = (f"https://www.linkedin.com/jobs/view/{jid}"
                       if str(jid).isdigit() else job["url"])
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
        li_state[jid] = {
            "title":    job["title"],
            "company":  job["company"],
            "url":      job["url"],
            "seen_at":  now.isoformat(),
            "relevant": result.get("relevant", False) if result else False,
        }
    return li_alerts


# ───────────────────────────────  MAIN  ──────────────────────────────

def main(dry_run: bool = False):
    """Full run. dry_run=True skips ALL state writes and email (read-only timing
    /diff harness) — used to compare against the local serial monitor safely."""
    companies = load_companies()
    if not companies:
        log.error("No companies loaded — check Notion token / companies.json")
        return
    log.info(f"Loaded {len(companies)} companies")

    state   = load_state()
    pending = load_pending()
    if pending:
        log.info(f"Resumed {len(pending)} pending alert(s) from an interrupted run")
    now = datetime.now()

    # ── Careers: collect (serial) → eval (concurrent) → apply (serial) ──
    t0 = time.time()
    payloads, direct_state = collect_careers_payloads(companies, state, now)
    log.info(f"Careers fetch done in {time.time()-t0:.0f}s — "
             f"{len(payloads)} companies with new jobs to evaluate")
    t1 = time.time()
    results = eval_careers_payloads(payloads, concurrent=True)
    log.info(f"Careers eval done in {time.time()-t1:.0f}s ({len(payloads)} evals)")
    if not dry_run:
        apply_careers_results(results, direct_state, state, pending)
        close_browser()
        save_state(state)
        save_pending(pending)

    # ── LinkedIn: collect (serial) → eval (concurrent) → apply (serial) ──
    log.info("Starting LinkedIn job search")
    t2 = time.time()
    li_jobs = collect_linkedin_jobs(state)
    log.info(f"LinkedIn search done in {time.time()-t2:.0f}s — "
             f"{len(li_jobs)} new jobs to evaluate")
    t3 = time.time()
    li_results = eval_linkedin_jobs(li_jobs, concurrent=True)
    log.info(f"LinkedIn eval done in {time.time()-t3:.0f}s ({len(li_jobs)} evals)")
    li_alerts = 0
    if not dry_run:
        li_alerts = apply_linkedin_results(li_results, state, pending, now)
        save_state(state)
        save_pending(pending)

    if dry_run:
        log.info(f"[DRY RUN] total {time.time()-t0:.0f}s — "
                 f"{len(payloads)} careers evals, {len(li_jobs)} LinkedIn evals; "
                 f"no state/email written")
        close_browser()
        return

    # ── One daily digest email — at most once per calendar day (same as monitor.py) ──
    today = datetime.now().strftime("%Y-%m-%d")
    if monitor._LLM_BALANCE_ERR:
        msg = ("DeepSeek API 余额不足(402 Insufficient Balance),今天所有岗位都没能评估。"
               "岗位已暂存待重评——请充值后重跑,不会漏。")
        log.error(msg)
        notify_email("[Job Monitor] ⚠️ DeepSeek 余额不足 — 请充值", msg)
        notify_macos("Job Monitor 余额不足", "DeepSeek 余额用尽,请充值后重跑")
    elif state.get("last_email_date") == today:
        log.info(f"Email already sent today ({today}); holding {len(pending)} alert(s).")
    elif send_daily_summary(pending, len(companies)):
        state["last_email_date"] = today
        save_state(state)
        clear_pending()
    else:
        log.warning("Email not delivered — keeping pending.json for next run's digest")
    log.info(f"Done — {len(pending)} alert(s) ({li_alerts} from LinkedIn). "
             f"{len(companies)} companies checked.")


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
