"""Equivalence test: prove monitor_concurrent's CONCURRENT eval path produces
byte-identical results to its SERIAL path, and that the apply stage matches the
hand-written expectation of monitor.py's semantics.

DeepSeek is replaced with a DETERMINISTIC stub so any difference is caused purely
by the concurrent scheduling / refactor — never by LLM jitter. Run:

    python3 test_equivalence.py
"""
import copy
import monitor
import monitor_concurrent as mc


def _sorted_pending(p):
    """Order-independent view of a pending list (concurrent completion order
    must not matter)."""
    return sorted((d["company"], d["roles"], d.get("source"),
                   tuple(d.get("job_links", []))) for d in p)


# ─────────────────────────  CAREERS  ─────────────────────────

def _careers_payloads():
    """Three companies covering the three eval branches."""
    return [
        {   # relevant → one pending, seen fully updated
            "name": "Rel", "url": "https://rel.example/careers", "seen": {"old"},
            "current_sigs": {"old", "gh:1"}, "kept": ["gh:1"],
            "new_display": ["Quant Researcher"], "jd_ctx": {},
            "disp_to_sig": {"Quant Researcher": "gh:1"},
            "job_url_snap": {"gh:1": "https://rel.example/jobs/1"},
            "base": {"hash": "h", "url": "https://rel.example/careers",
                     "notion_page_id": "pRel", "last_checked": "t"},
        },
        {   # not relevant → no pending, seen fully updated
            "name": "NoRel", "url": "https://norel.example/careers", "seen": set(),
            "current_sigs": {"gh:2"}, "kept": ["gh:2"],
            "new_display": ["Sales Manager"], "jd_ctx": {},
            "disp_to_sig": {"Sales Manager": "gh:2"}, "job_url_snap": {},
            "base": {"hash": "h", "url": "https://norel.example/careers",
                     "notion_page_id": "pNo", "last_checked": "t"},
        },
        {   # eval fails (None) → no pending, kept HELD (not marked seen)
            "name": "Fail", "url": "https://fail.example/careers", "seen": {"old2"},
            "current_sigs": {"old2", "gh:3"}, "kept": ["gh:3"],
            "new_display": ["Quant Trader"], "jd_ctx": {},
            "disp_to_sig": {"Quant Trader": "gh:3"}, "job_url_snap": {},
            "base": {"hash": "h", "url": "https://fail.example/careers",
                     "notion_page_id": "pFail", "last_checked": "t"},
        },
    ]


def _careers_stub(company, added_lines, jd_ctx=None):
    if company == "Rel":
        return {"relevant": True, "roles": ["Quant Researcher"],
                "matched_lines": list(added_lines), "reason": "match"}
    if company == "NoRel":
        return {"relevant": False, "roles": [], "matched_lines": [], "reason": ""}
    return None   # "Fail" → LLM eval failed


def test_careers_equivalence():
    monitor.evaluate = _careers_stub   # monitor_concurrent calls monitor.evaluate
    direct = {"Seed": {"seen_jobs": ["gh:9"], "hash": "h", "url": "u",
                       "notion_page_id": "pSeed", "last_checked": "t"}}

    def run(concurrent):
        payloads = _careers_payloads()
        results = mc.eval_careers_payloads(payloads, concurrent=concurrent)
        state, pending = {}, []
        mc.apply_careers_results(results, copy.deepcopy(direct), state, pending)
        return state, pending

    s_state, s_pending = run(False)
    c_state, c_pending = run(True)

    assert s_state == c_state, "careers state differs serial vs concurrent"
    assert _sorted_pending(s_pending) == _sorted_pending(c_pending), \
        "careers pending differs serial vs concurrent"

    # Hand-checked expectations of monitor.py semantics:
    assert len(c_pending) == 1 and c_pending[0]["company"] == "Rel", \
        "only the relevant company should queue a pending alert"
    assert c_pending[0]["job_links"] == ["https://rel.example/jobs/1"], \
        "relevant alert must carry its snapshotted per-job link"
    assert c_state["Rel"]["seen_jobs"] == ["gh:1", "old"], \
        "relevant company: all current sigs marked seen"
    assert c_state["NoRel"]["seen_jobs"] == ["gh:2"], \
        "not-relevant company: still marked seen"
    assert c_state["Fail"]["seen_jobs"] == ["old2"], \
        "eval-failed company: new job HELD (not marked seen) for retry"
    assert c_state["Seed"]["seen_jobs"] == ["gh:9"], "direct_state passthrough"
    print("PASS careers: serial == concurrent, apply matches monitor.py semantics")


# ─────────────────────────  LINKEDIN  ─────────────────────────

def _li_jobs():
    return [
        {"id": "111", "title": "Quant Researcher", "company": "Alpha",
         "url": "https://linkedin.com/jobs/view/111", "location": "United States"},
        {"id": "222", "title": "Sales Rep", "company": "Beta",
         "url": "https://linkedin.com/jobs/view/222", "location": "United Kingdom"},
        {"id": "333", "title": "Quant Dev", "company": "Gamma",
         "url": "https://linkedin.com/jobs/view/333", "location": "Singapore"},
    ]


def _li_stub(job):
    if job["id"] == "111":
        return {"relevant": True, "roles": ["Quant Researcher"], "reason": "m"}
    if job["id"] == "222":
        return {"relevant": False}
    return {"_eval_failed": True}   # "333" transient failure


def test_linkedin_equivalence():
    monitor.evaluate_linkedin_job = _li_stub
    now = monitor.datetime.now()

    def run(concurrent):
        jobs = _li_jobs()
        results = mc.eval_linkedin_jobs(jobs, concurrent=concurrent)
        state = {"_linkedin_seen": {}}
        pending = []
        alerts = mc.apply_linkedin_results(results, state, pending, now)
        return state["_linkedin_seen"], pending, alerts

    s_li, s_pending, s_alerts = run(False)
    c_li, c_pending, c_alerts = run(True)

    assert s_li == c_li, "li_state differs serial vs concurrent"
    assert _sorted_pending(s_pending) == _sorted_pending(c_pending), \
        "LinkedIn pending differs serial vs concurrent"
    assert s_alerts == c_alerts == 1, "exactly one LinkedIn alert (id 111)"

    # Hand-checked monitor.py semantics:
    assert "111" in c_li and c_li["111"]["relevant"] is True
    assert "222" in c_li and c_li["222"]["relevant"] is False, \
        "not-relevant job frozen in li_state (relevant=False), never re-evaluated"
    assert "333" not in c_li, \
        "eval-failed job NOT recorded → re-evaluated next run"
    assert c_pending[0]["url"] == "https://www.linkedin.com/jobs/view/111", \
        "canonical numeric view URL"
    print("PASS linkedin: serial == concurrent, apply matches monitor.py semantics")


def test_linkedin_collect_dedup():
    """collect_linkedin_jobs: same id across searches evaluated once; ids already
    frozen in _linkedin_seen are skipped."""
    calls = {"n": 0}
    dup = {"id": "555", "title": "Quant", "company": "X",
           "url": "https://linkedin.com/jobs/view/555", "location": "US"}
    frozen = {"id": "999", "title": "Old", "company": "Y",
              "url": "https://linkedin.com/jobs/view/999", "location": "US"}

    def fake_search(kw, loc):
        calls["n"] += 1
        return [dup, frozen], False   # (jobs, throttled): every search returns the same two

    orig = mc.search_linkedin_gentle
    mc.search_linkedin_gentle = fake_search
    orig_sleep = mc.time.sleep
    mc.time.sleep = lambda *_: None   # don't actually wait 70×1s
    try:
        state = {"_linkedin_seen": {"999": {"relevant": False}}}
        collected = mc.collect_linkedin_jobs(state)
    finally:
        mc.search_linkedin_gentle = orig
        mc.time.sleep = orig_sleep

    ids = [j["id"] for j in collected]
    assert ids == ["555"], f"expected only new id 555 once, got {ids}"
    assert calls["n"] == len(mc.LINKEDIN_KEYWORDS) * len(mc.LINKEDIN_LOCATIONS)
    print("PASS linkedin collect: cross-search dedup + frozen-id skip")


def test_linkedin_throttle_rerun():
    """A search throttled on its first pass is re-run after cooldown and its
    jobs are recovered — the fix for today's silent-drop-on-429 bug."""
    orig_kw, orig_loc = mc.LINKEDIN_KEYWORDS, mc.LINKEDIN_LOCATIONS
    mc.LINKEDIN_KEYWORDS, mc.LINKEDIN_LOCATIONS = ["k1"], ["L1"]
    calls = {}
    late_job = {"id": "777", "title": "Quant Researcher", "company": "Z",
                "url": "https://linkedin.com/jobs/view/777", "location": "L1", "posted": ""}

    def fake(kw, loc):
        k = kw + loc
        calls[k] = calls.get(k, 0) + 1
        if calls[k] == 1:
            return [], True        # first pass: throttled, dropped nothing recoverable yet
        return [late_job], False   # re-run after cooldown: recovers the missed job

    orig_search, orig_sleep = mc.search_linkedin_gentle, mc.time.sleep
    mc.search_linkedin_gentle = fake
    mc.time.sleep = lambda *_: None
    try:
        collected = mc.collect_linkedin_jobs({"_linkedin_seen": {}})
    finally:
        mc.search_linkedin_gentle, mc.time.sleep = orig_search, orig_sleep
        mc.LINKEDIN_KEYWORDS, mc.LINKEDIN_LOCATIONS = orig_kw, orig_loc

    assert [j["id"] for j in collected] == ["777"], \
        "throttled search must be re-run and recover its jobs"
    assert calls["k1L1"] == 2, "throttled search must be re-run exactly once"
    print("PASS linkedin throttle: throttled search re-run recovers missed jobs")


if __name__ == "__main__":
    test_careers_equivalence()
    test_linkedin_equivalence()
    test_linkedin_collect_dedup()
    test_linkedin_throttle_rerun()
    print("\nALL EQUIVALENCE TESTS PASSED")
