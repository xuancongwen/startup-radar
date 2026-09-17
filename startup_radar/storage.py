"""Single-process SQLite work queue and restart-recoverable daily CSV export."""
import csv
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .scoring import Analysis

FIELDS = ["domain", "first_seen_at", "resolved_url", "title", "description", "score",
          "matched_signals", "status"]


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def csv_safe(value: object) -> object:
    if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


class Store:
    def __init__(self, path: Path, export_dir: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        export_dir.mkdir(parents=True, exist_ok=True)
        self.exports = export_dir
        self.db = sqlite3.connect(path, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS domains (
                domain TEXT PRIMARY KEY, first_seen_at TEXT NOT NULL,
                resolved_url TEXT, title TEXT, description TEXT,
                score INTEGER NOT NULL DEFAULT 0,
                matched_signals TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'pending',
                og_tags TEXT NOT NULL DEFAULT '{}',
                checked_at TEXT, shortlist_day TEXT, error TEXT
            );
            CREATE INDEX IF NOT EXISTS domains_status ON domains(status, first_seen_at);
            CREATE INDEX IF NOT EXISTS domains_shortlist ON domains(shortlist_day);
            CREATE INDEX IF NOT EXISTS domains_candidates ON domains(status, checked_at, domain);
            UPDATE domains SET status='pending' WHERE status='processing';
        """)
        self.rebuild_exports()

    def counts(self) -> dict[str, int]:
        return dict(self.db.execute("SELECT status, COUNT(*) FROM domains GROUP BY status"))

    def admit(self, domain: str) -> bool:
        with self.db:
            cursor = self.db.execute("INSERT OR IGNORE INTO domains(domain,first_seen_at) VALUES (?,?)",
                                     (domain, now()))
        return cursor.rowcount == 1

    def exists(self, domain: str) -> bool:
        return self.db.execute("SELECT 1 FROM domains WHERE domain=?", (domain,)).fetchone() is not None

    def claim(self) -> str | None:
        with self.db:
            row = self.db.execute("SELECT domain FROM domains WHERE status='pending' "
                                  "ORDER BY first_seen_at,domain LIMIT 1").fetchone()
            if row:
                self.db.execute("UPDATE domains SET status='processing' WHERE domain=?", (row[0],))
        return row[0] if row else None

    def finish(self, domain: str, url: str, result: Analysis) -> None:
        checked = now()
        day = checked[:10].replace("-", "") if result.status == "startup_candidate" else None
        with self.db:
            self.db.execute("""UPDATE domains SET resolved_url=?,title=?,description=?,score=?,
                matched_signals=?,status=?,og_tags=?,checked_at=?,shortlist_day=?,error=NULL
                WHERE domain=?""", (url, result.title, result.description, result.score,
                json.dumps(result.matched_signals), result.status, json.dumps(result.og_tags),
                checked, day, domain))
        if day:
            row = self.db.execute("SELECT * FROM domains WHERE domain=?", (domain,)).fetchone()
            self.append(day, row)

    def fail(self, domain: str, status: str, error: str) -> None:
        with self.db:
            self.db.execute("UPDATE domains SET status=?,error=?,checked_at=? WHERE domain=?",
                            (status, error[:500], now(), domain))

    def append(self, day: str, row: sqlite3.Row) -> None:
        path = self.exports / f"shortlist_{day}.csv"
        header = not path.exists() or path.stat().st_size == 0
        with path.open("a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            if header:
                writer.writerow(FIELDS)
            writer.writerow([csv_safe(row[field]) for field in FIELDS])
            file.flush()
            os.fsync(file.fileno())

    def rebuild_exports(self) -> None:
        # DB is the source of truth. Atomic rebuild repairs a crash between DB
        # commit and CSV append, partial appends, and moved/deleted CSV files.
        for (day,) in self.db.execute("SELECT DISTINCT shortlist_day FROM domains WHERE shortlist_day IS NOT NULL"):
            path = self.exports / f"shortlist_{day}.csv"
            temp = path.with_suffix(".csv.tmp")
            with temp.open("w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(FIELDS)
                for row in self.db.execute("SELECT * FROM domains WHERE shortlist_day=? ORDER BY domain", (day,)):
                    writer.writerow([csv_safe(row[field]) for field in FIELDS])
                file.flush()
                os.fsync(file.fileno())
            temp.replace(path)

    def close(self) -> None:
        self.db.close()
