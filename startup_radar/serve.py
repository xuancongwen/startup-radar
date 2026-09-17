"""Read-only HTTP endpoint over the radar database for downstream consumers.

One process, standard library only, no writes. It opens the SQLite file read-only for every
request, so it runs beside the pipeline without sharing a lock. Clients page through
startup candidates with an opaque cursor and never see pending, live, parked, or failed rows.
"""
import argparse
import hmac
import html
import json
import logging
import os
import signal
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

LOG = logging.getLogger("startup_radar.serve")
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
CURSOR_SEP = "|"
STALE_AFTER = timedelta(minutes=10)  # the status page flags a pipeline quiet for longer
HISTOGRAM_DAYS = 14
CANDIDATE_FIELDS = ["domain", "first_seen_at", "checked_at", "shortlist_day", "resolved_url",
                    "title", "description", "score", "matched_signals", "og_tags"]


class Tokens:
    """Bearer tokens as label:secret pairs. The label names the client in logs; the secret never appears."""

    def __init__(self, pairs: list[tuple[str, str]]):
        self.pairs = pairs

    @classmethod
    def parse(cls, raw: str) -> "Tokens":
        pairs = []
        for item in raw.replace("\n", ",").split(","):
            item = item.strip()
            if not item:
                continue
            label, sep, secret = item.partition(":")
            if not sep or not label.strip() or len(secret.strip()) < 16:
                raise ValueError("each token must be label:secret with a secret of at least 16 characters")
            pairs.append((label.strip(), secret.strip()))
        return cls(pairs)

    def label_for(self, presented: str) -> str | None:
        found = None
        for label, secret in self.pairs:  # constant-time per pair; no early exit on match
            if hmac.compare_digest(secret.encode(), presented.encode()):
                found = label
        return found


class Cursor:
    """Position after (checked_at, domain), which is unique and matches the query order."""

    @staticmethod
    def encode(checked_at: str, domain: str) -> str:
        return f"{checked_at}{CURSOR_SEP}{domain}"

    @staticmethod
    def decode(raw: str) -> tuple[str, str]:
        checked_at, sep, domain = raw.partition(CURSOR_SEP)
        if not sep or not domain or CURSOR_SEP in domain:
            raise ValueError("malformed cursor")
        try:
            datetime.fromisoformat(checked_at)
        except ValueError as error:
            raise ValueError("malformed cursor") from error
        return checked_at, domain


class Config:
    def __init__(self, db: Path, tokens: Tokens | None, min_score: int, max_limit: int = MAX_LIMIT):
        self.db = db
        self.tokens = tokens  # None means anonymous access was explicitly allowed
        self.min_score = min_score
        self.max_limit = max_limit


def open_readonly(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(path)
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    return connection


def query_candidates(connection: sqlite3.Connection, min_score: int, after: tuple[str, str] | None,
                     limit: int, day: str | None) -> tuple[list[dict], str | None]:
    checked_at, domain = after or ("", "")
    params: list[object] = ["startup_candidate", min_score, checked_at, domain]
    day_clause = ""
    if day:
        day_clause = " AND shortlist_day = ?"
        params.append(day)
    params.append(limit + 1)
    rows = connection.execute(
        f"SELECT {', '.join(CANDIDATE_FIELDS)} FROM domains"
        f" WHERE status = ? AND score >= ? AND (checked_at, domain) > (?, ?){day_clause}"
        " ORDER BY checked_at, domain LIMIT ?", params).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    items = []
    for row in rows:
        item = dict(row)
        item["matched_signals"] = json.loads(item["matched_signals"] or "[]")
        item["og_tags"] = json.loads(item["og_tags"] or "{}")
        items.append(item)
    next_cursor = Cursor.encode(rows[-1]["checked_at"], rows[-1]["domain"]) if rows and has_more else None
    return items, next_cursor


def query_stats(connection: sqlite3.Connection) -> dict:
    counts = dict(connection.execute("SELECT status, COUNT(*) FROM domains GROUP BY status"))
    (last_checked,) = connection.execute("SELECT MAX(checked_at) FROM domains").fetchone()
    (last_candidate,) = connection.execute(
        "SELECT MAX(checked_at) FROM domains WHERE status = 'startup_candidate'").fetchone()
    return {"counts": counts, "total": sum(counts.values()),
            "last_checked_at": last_checked, "last_candidate_at": last_candidate}


def query_status(connection: sqlite3.Connection, now: datetime) -> dict:
    stats = query_stats(connection)
    since = (now - timedelta(days=1)).isoformat(timespec="seconds")
    (admitted_24h,) = connection.execute(
        "SELECT COUNT(*) FROM domains WHERE first_seen_at >= ?", (since,)).fetchone()
    (candidates_24h,) = connection.execute(
        "SELECT COUNT(*) FROM domains WHERE status = 'startup_candidate' AND checked_at >= ?", (since,)).fetchone()
    days = connection.execute(
        "SELECT shortlist_day, COUNT(*) FROM domains WHERE shortlist_day IS NOT NULL"
        " GROUP BY shortlist_day ORDER BY shortlist_day DESC LIMIT ?", (HISTOGRAM_DAYS,)).fetchall()
    last = stats["last_checked_at"]
    age = (now - datetime.fromisoformat(last)) if last else None
    return {**stats, "admitted_24h": admitted_24h, "candidates_24h": candidates_24h,
            "days": [(day, count) for day, count in reversed(days)],
            "age_seconds": None if age is None else max(0, int(age.total_seconds())),
            "stale": age is None or age > STALE_AFTER}


def humanize(seconds: int | None) -> str:
    if seconds is None:
        return "never"
    if seconds < 90:
        return f"{seconds}s ago"
    if seconds < 5400:
        return f"{seconds // 60}m ago"
    if seconds < 172800:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


STATUS_ORDER = ["startup_candidate", "live", "parked", "rejected", "dns_failed", "fetch_failed", "pending", "processing"]

STATUS_CSS = """
:root{color-scheme:light dark;--bg:#fff;--fg:#1a1a1a;--muted:#666;--line:#ddd;--ok:#1a7f37;--bad:#b42318;--bar:#4a6fa5}
@media(prefers-color-scheme:dark){:root{--bg:#111;--fg:#eee;--muted:#999;--line:#333;--ok:#3fb950;--bad:#f85149;--bar:#6b93d6}}
body{margin:0;padding:24px 16px;background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,sans-serif;max-width:720px;margin-inline:auto}
h1{font-size:20px;margin:0 0 4px}h2{font-size:15px;margin:28px 0 8px;color:var(--muted);font-weight:600}
.pill{display:inline-block;padding:2px 10px;border-radius:999px;font-size:13px;font-weight:600;color:#fff}
.ok{background:var(--ok)}.bad{background:var(--bad)}
.muted{color:var(--muted)}table{border-collapse:collapse;width:100%}td,th{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line)}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
.bars{display:grid;grid-template-columns:auto 1fr auto;gap:4px 10px;align-items:center;font-variant-numeric:tabular-nums}
.bar{height:14px;background:var(--bar);border-radius:3px;min-width:2px}
footer{margin-top:32px;font-size:13px;color:var(--muted)}
"""


def render_status(status: dict, now: datetime, min_score: int) -> str:
    e = html.escape
    fresh = not status["stale"]
    pill = f'<span class="pill {"ok" if fresh else "bad"}">{"running" if fresh else "stale"}</span>'
    rows = "".join(
        f'<tr><td>{e(name.replace("_", " "))}</td><td class="n">{status["counts"].get(name, 0):,}</td></tr>'
        for name in STATUS_ORDER if name in status["counts"])
    rows += "".join(f'<tr><td>{e(name)}</td><td class="n">{count:,}</td></tr>'
                    for name, count in sorted(status["counts"].items()) if name not in STATUS_ORDER)
    peak = max((count for _, count in status["days"]), default=1)
    bars = "".join(
        f'<div class="muted">{e(day[:4])}-{e(day[4:6])}-{e(day[6:])}</div>'
        f'<div><div class="bar" style="width:{max(1, count * 100 // peak)}%"></div></div><div class="n">{count:,}</div>'
        for day, count in status["days"]) or '<div class="muted" style="grid-column:1/-1">no candidates yet</div>'
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="60">
<title>Startup Radar status</title><style>{STATUS_CSS}</style></head><body>
<h1>Startup Radar {pill}</h1>
<div class="muted">last check {e(humanize(status["age_seconds"]))}{" · last candidate " + e(status["last_candidate_at"]) if status["last_candidate_at"] else ""}</div>
<h2>Last 24 hours</h2>
<table><tr><td>domains admitted</td><td class="n">{status["admitted_24h"]:,}</td></tr>
<tr><td>startup candidates</td><td class="n">{status["candidates_24h"]:,}</td></tr></table>
<h2>Candidates per day (UTC)</h2>
<div class="bars">{bars}</div>
<h2>All rows by status</h2>
<table>{rows}<tr><th>total</th><th class="n">{status["total"]:,}</th></tr></table>
<footer>API serves candidates with score ≥ {min_score}. Rendered {e(now.isoformat(timespec="seconds"))}; refreshes every minute.
Domain names are not shown here because this page needs no token; use <code>/candidates</code> with a bearer token.</footer>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    server_version = "StartupRadar/0.2"
    sys_version = ""
    protocol_version = "HTTP/1.1"
    config: Config  # set on the server class before serving

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        LOG.info("%s %s", self.address_string(), format % args)

    def send_json(self, status: HTTPStatus, body: dict) -> None:
        payload = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def send_html(self, body: str) -> None:
        payload = body.encode()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def error(self, status: HTTPStatus, message: str) -> None:
        self.send_json(status, {"error": message})

    def authorized(self) -> bool:
        tokens = self.config.tokens
        if tokens is None:
            return True
        header = self.headers.get("Authorization", "")
        scheme, _, presented = header.partition(" ")
        label = tokens.label_for(presented.strip()) if scheme.lower() == "bearer" and presented else None
        if label is None:
            self.send_response(HTTPStatus.UNAUTHORIZED)
            self.send_header("WWW-Authenticate", 'Bearer realm="startup-radar"')
            payload = b'{"error": "missing or invalid bearer token"}'
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(payload)
            return False
        self.client_label = label
        return True

    def do_GET(self) -> None:  # noqa: N802 - stdlib naming
        url = urlsplit(self.path)
        query = parse_qs(url.query, keep_blank_values=False)
        routes = {"/": self.route_index, "/health": self.route_health, "/status": self.route_status,
                  "/stats": self.route_stats, "/candidates": self.route_candidates}
        handler = routes.get(url.path.rstrip("/") or "/")
        if handler is None:
            return self.error(HTTPStatus.NOT_FOUND, "no such route")
        try:
            handler(query)
        except FileNotFoundError:
            self.error(HTTPStatus.SERVICE_UNAVAILABLE, "database not ready")
        except sqlite3.Error as error:
            LOG.error("sqlite error: %s", error)
            self.error(HTTPStatus.SERVICE_UNAVAILABLE, "database unavailable")

    def do_HEAD(self) -> None:  # noqa: N802 - same headers as GET, no body
        self.do_GET()

    def do_POST(self) -> None:  # noqa: N802
        self.error(HTTPStatus.METHOD_NOT_ALLOWED, "read-only service")

    do_PUT = do_DELETE = do_PATCH = do_POST

    def route_index(self, query: dict) -> None:
        if "text/html" in self.headers.get("Accept", ""):  # a browser: show the status page
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", "/status")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_json(HTTPStatus.OK, {
            "service": "startup-radar", "routes": {
                "/health": "liveness, no auth",
                "/status": "human status page, no auth",
                "/stats": "row counts and last activity, bearer auth",
                "/candidates": "startup candidates, bearer auth; params: cursor, limit, min_score, day"}})

    def route_health(self, query: dict) -> None:
        with open_readonly(self.config.db) as connection:
            (last,) = connection.execute("SELECT MAX(checked_at) FROM domains").fetchone()
        self.send_json(HTTPStatus.OK, {"status": "ok", "last_checked_at": last})

    def route_status(self, query: dict) -> None:
        now = datetime.now(timezone.utc)
        with open_readonly(self.config.db) as connection:
            status = query_status(connection, now)
        self.send_html(render_status(status, now, self.config.min_score))

    def route_stats(self, query: dict) -> None:
        if not self.authorized():
            return
        with open_readonly(self.config.db) as connection:
            self.send_json(HTTPStatus.OK, query_stats(connection))

    def route_candidates(self, query: dict) -> None:
        if not self.authorized():
            return
        try:
            limit = int(query.get("limit", [DEFAULT_LIMIT])[0])
            client_min = int(query.get("min_score", [0])[0])
        except ValueError:
            return self.error(HTTPStatus.BAD_REQUEST, "limit and min_score must be integers")
        if limit < 1:
            return self.error(HTTPStatus.BAD_REQUEST, "limit must be positive")
        limit = min(limit, self.config.max_limit)
        min_score = max(self.config.min_score, client_min)  # a client may raise the floor, never lower it
        day = query.get("day", [None])[0]
        if day is not None and not (len(day) == 8 and day.isdigit()):
            return self.error(HTTPStatus.BAD_REQUEST, "day must be YYYYMMDD")
        after = None
        if "cursor" in query:
            try:
                after = Cursor.decode(query["cursor"][0])
            except ValueError as error:
                return self.error(HTTPStatus.BAD_REQUEST, str(error))
        with open_readonly(self.config.db) as connection:
            items, next_cursor = query_candidates(connection, min_score, after, limit, day)
        self.send_json(HTTPStatus.OK, {
            "candidates": items, "next_cursor": next_cursor, "min_score": min_score, "limit": limit})


def make_server(config: Config, host: str, port: int) -> ThreadingHTTPServer:
    class BoundHandler(Handler):
        pass
    BoundHandler.config = config
    server = ThreadingHTTPServer((host, port), BoundHandler)
    server.daemon_threads = True
    return server


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", type=Path, default=Path("data/radar.sqlite3"))
    p.add_argument("--host", default="127.0.0.1", help="bind address; put TLS in front before leaving localhost")
    p.add_argument("--port", type=int, default=8081)
    p.add_argument("--tokens", default=os.environ.get("RADAR_TOKENS", ""),
                   help="comma-separated label:secret pairs; default from RADAR_TOKENS")
    p.add_argument("--tokens-file", type=Path, help="file of label:secret pairs, one per line")
    p.add_argument("--allow-anonymous", action="store_true",
                   help="serve without tokens; only for a host firewall you trust")
    p.add_argument("--min-score", type=int, default=int(os.environ.get("RADAR_MIN_SCORE", "0")),
                   help="lowest candidate score served; clients may ask for higher, never lower")
    p.add_argument("--max-limit", type=int, default=MAX_LIMIT)
    return p


def load_config(args: argparse.Namespace) -> Config:
    raw = args.tokens
    if args.tokens_file:
        raw = args.tokens_file.read_text()
    tokens = Tokens.parse(raw)
    if not tokens.pairs and not args.allow_anonymous:
        raise ValueError("no tokens configured; set RADAR_TOKENS, --tokens-file, or pass --allow-anonymous")
    if args.min_score < 0 or args.max_limit < 1:
        raise ValueError("min-score must be nonnegative and max-limit positive")
    return Config(args.db.resolve(), None if args.allow_anonymous and not tokens.pairs else tokens,
                  args.min_score, args.max_limit)


def main() -> None:
    args = parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        config = load_config(args)
    except ValueError as error:
        sys.exit(f"error: {error}")
    server = make_server(config, args.host, args.port)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    LOG.info("serving %s on http://%s:%d/ min_score=%d clients=%s", config.db, args.host, args.port,
             config.min_score, "anonymous" if config.tokens is None else [l for l, _ in config.tokens.pairs])
    try:
        server.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
