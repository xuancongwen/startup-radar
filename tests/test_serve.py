import argparse
import json
import threading
from pathlib import Path

import httpx
import pytest

from startup_radar.scoring import Analysis
from startup_radar.serve import Config, Cursor, Tokens, load_config, make_server
from startup_radar.storage import Store

SECRET = "s3cret-token-for-tests"


def seed(tmp_path: Path) -> Path:
    db = tmp_path / "radar.sqlite3"
    store = Store(db, tmp_path / "csv")
    rows = [
        ("alpha.com", 12, "startup_candidate"), ("beta.io", 8, "startup_candidate"),
        ("gamma.ai", 15, "startup_candidate"), ("delta.dev", 9, "startup_candidate"),
        ("live.com", 5, "live"), ("parked.com", 0, "parked"),
    ]
    for domain, score, status in rows:
        store.admit(domain)
        store.finish(domain, f"https://{domain}/", Analysis(
            f"Title {domain}", "desc", {"og:title": domain}, "", score,
            [{"category": "tech_product", "phrase": "API", "points": 3}], status))
    store.admit("failed.com")
    store.fail("failed.com", "dns_failed", "no answer")
    # Distinct seconds for two rows, a shared second for the other two, so paging must break ties by domain.
    with store.db:
        store.db.execute("UPDATE domains SET checked_at='2026-09-14T10:00:00+00:00' WHERE domain='alpha.com'")
        store.db.execute("UPDATE domains SET checked_at='2026-09-14T11:00:00+00:00' WHERE domain IN ('beta.io','gamma.ai')")
        store.db.execute("UPDATE domains SET checked_at='2026-09-14T12:00:00+00:00', shortlist_day='20260915' WHERE domain='delta.dev'")
    store.close()
    return db


@pytest.fixture
def served(tmp_path):
    db = seed(tmp_path)
    server = make_server(Config(db, Tokens.parse(f"acme:{SECRET}"), min_score=0), "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    with httpx.Client(base_url=base, headers={"Authorization": f"Bearer {SECRET}"}) as client:
        yield client, db
    server.shutdown()
    server.server_close()


def test_tokens_parse_and_lookup():
    tokens = Tokens.parse("acme:aaaaaaaaaaaaaaaa, beta:bbbbbbbbbbbbbbbbbb\n")
    assert tokens.label_for("bbbbbbbbbbbbbbbbbb") == "beta"
    assert tokens.label_for("aaaaaaaaaaaaaaaa") == "acme"
    assert tokens.label_for("nope") is None
    with pytest.raises(ValueError):
        Tokens.parse("acme:short")
    with pytest.raises(ValueError):
        Tokens.parse("nolabel")


def test_refuses_to_start_without_tokens(tmp_path):
    args = argparse.Namespace(tokens="", tokens_file=None, allow_anonymous=False, min_score=0,
                              max_limit=10, db=tmp_path / "x")
    with pytest.raises(ValueError):
        load_config(args)
    args.allow_anonymous = True
    assert load_config(args).tokens is None


def test_cursor_roundtrip_and_rejects_garbage():
    assert Cursor.decode(Cursor.encode("2026-09-14T10:00:00+00:00", "a.com")) == ("2026-09-14T10:00:00+00:00", "a.com")
    for bad in ["", "nodivider", "notadate|a.com", "2026-09-14T10:00:00+00:00|", "2026-09-14T10:00:00+00:00|a|b"]:
        with pytest.raises(ValueError):
            Cursor.decode(bad)


def test_health_is_open_and_index_lists_routes(served):
    client, _ = served
    anonymous = httpx.get(str(client.base_url) + "/health")
    assert anonymous.status_code == 200 and anonymous.json()["status"] == "ok"
    assert anonymous.headers["cache-control"] == "no-store"
    assert "/candidates" in httpx.get(str(client.base_url) + "/").json()["routes"]


def test_auth_required_for_data(served):
    client, _ = served
    for path in ["/candidates", "/stats"]:
        r = httpx.get(str(client.base_url) + path)
        assert r.status_code == 401 and r.headers["www-authenticate"].startswith("Bearer")
        assert httpx.get(str(client.base_url) + path, headers={"Authorization": "Bearer wrong"}).status_code == 401
        assert httpx.get(str(client.base_url) + path, headers={"Authorization": f"Basic {SECRET}"}).status_code == 401
    assert client.get("/stats").status_code == 200


def test_only_candidates_and_ordered(served):
    client, _ = served
    body = client.get("/candidates").json()
    assert [c["domain"] for c in body["candidates"]] == ["alpha.com", "beta.io", "gamma.ai", "delta.dev"]
    assert body["next_cursor"] is None
    first = body["candidates"][0]
    assert first["matched_signals"] == [{"category": "tech_product", "phrase": "API", "points": 3}]
    assert first["og_tags"] == {"og:title": "alpha.com"}
    assert set(first) == {"domain", "first_seen_at", "checked_at", "shortlist_day", "resolved_url",
                          "title", "description", "score", "matched_signals", "og_tags"}


def test_pagination_breaks_ties_and_terminates(served):
    client, _ = served
    seen, cursor, pages = [], None, 0
    while True:
        params = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        body = client.get("/candidates", params=params).json()
        pages += 1
        seen += [c["domain"] for c in body["candidates"]]
        cursor = body["next_cursor"]
        if not cursor:
            break
    assert seen == ["alpha.com", "beta.io", "gamma.ai", "delta.dev"] and pages == 2
    # A cursor pointing at the last row yields an empty page and no further cursor.
    tail = client.get("/candidates", params={"cursor": Cursor.encode("2026-09-14T12:00:00+00:00", "delta.dev")}).json()
    assert tail["candidates"] == [] and tail["next_cursor"] is None


def test_filters_and_limits(served):
    client, _ = served
    assert [c["domain"] for c in client.get("/candidates", params={"min_score": 10}).json()["candidates"]] == ["alpha.com", "gamma.ai"]
    assert [c["domain"] for c in client.get("/candidates", params={"day": "20260915"}).json()["candidates"]] == ["delta.dev"]
    assert client.get("/candidates", params={"limit": 5000}).json()["limit"] == 1000
    for params in [{"limit": 0}, {"limit": "x"}, {"min_score": "y"}, {"day": "2026-9-1"}, {"cursor": "junk"}]:
        assert client.get("/candidates", params=params).status_code == 400


def test_server_floor_wins_over_client(tmp_path):
    db = seed(tmp_path)
    server = make_server(Config(db, None, min_score=10), "127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        body = httpx.get(f"http://127.0.0.1:{server.server_address[1]}/candidates", params={"min_score": 1}).json()
        assert body["min_score"] == 10 and [c["domain"] for c in body["candidates"]] == ["alpha.com", "gamma.ai"]
    finally:
        server.shutdown()
        server.server_close()


def test_stats_and_write_methods(served):
    client, _ = served
    stats = client.get("/stats").json()
    assert stats["counts"] == {"startup_candidate": 4, "live": 1, "parked": 1, "dns_failed": 1} and stats["total"] == 7
    assert stats["last_candidate_at"] == "2026-09-14T12:00:00+00:00"
    assert client.post("/candidates").status_code == 405
    assert client.get("/nope").status_code == 404


def test_missing_database_is_503(tmp_path):
    server = make_server(Config(tmp_path / "absent.sqlite3", None, min_score=0), "127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        r = httpx.get(f"http://127.0.0.1:{server.server_address[1]}/health")
        assert r.status_code == 503 and "not ready" in r.json()["error"]
    finally:
        server.shutdown()
        server.server_close()


def test_readonly_connection_cannot_write(served):
    from startup_radar.serve import open_readonly
    _, db = served
    with open_readonly(db) as connection, pytest.raises(Exception):
        connection.execute("DELETE FROM domains")


def test_status_page_is_open_and_flags_stale_pipeline(served):
    client, db = served
    import sqlite3
    from startup_radar.storage import now
    with sqlite3.connect(db) as connection:  # every row days old, so the pipeline reads as quiet
        connection.execute("UPDATE domains SET checked_at='2026-09-14T09:00:00+00:00' WHERE checked_at > '2026-09-14T12:00:00+00:00'")
    r = httpx.get(str(client.base_url) + "/status")
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/html")
    assert "default-src 'none'" in r.headers["content-security-policy"]
    page = r.text
    assert "stale" in page and "startup candidate" in page and "2026-09-15" in page  # seeded rows are days old
    assert "alpha.com" not in page  # no domain names without a token
    # A fresh check flips the badge.
    with sqlite3.connect(db) as connection:
        connection.execute("UPDATE domains SET checked_at=? WHERE domain='live.com'", (now(),))
    page = httpx.get(str(client.base_url) + "/status").text
    assert ">running<" in page and ">stale<" not in page


def test_root_redirects_browsers_only(served):
    client, _ = served
    browser = httpx.get(str(client.base_url) + "/", headers={"Accept": "text/html,*/*"})
    assert browser.status_code == 302 and browser.headers["location"] == "/status"
    assert "routes" in httpx.get(str(client.base_url) + "/").json()


def test_status_page_with_empty_database(tmp_path):
    db = tmp_path / "empty.sqlite3"
    Store(db, tmp_path / "csv").close()
    server = make_server(Config(db, None, min_score=0), "127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        page = httpx.get(f"http://127.0.0.1:{server.server_address[1]}/status").text
        assert "never" in page and "no candidates yet" in page and ">stale<" in page
    finally:
        server.shutdown()
        server.server_close()


def test_head_matches_get_without_body(served):
    client, _ = served
    for path, status in [("/status", 200), ("/health", 200), ("/candidates", 401), ("/nope", 404)]:
        r = httpx.head(str(client.base_url) + path)
        assert r.status_code == status and r.content == b"", path
        assert r.headers["cache-control"] == "no-store"
    assert httpx.head(str(client.base_url) + "/status").headers["content-type"].startswith("text/html")
