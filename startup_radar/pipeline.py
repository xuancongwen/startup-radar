"""Run with python -m startup_radar.pipeline --help."""
import argparse
import asyncio
import json
import logging
import random
import signal
from collections import Counter
from pathlib import Path

import httpx
from filelock import FileLock, Timeout as LockTimeout
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from .domains import apex_domain, certificate_domains
from .fetch import DNSFailure, Fetcher, Rejected
from .scoring import analyze
from .storage import Store

LOG = logging.getLogger("startup_radar")


class Admission:
    def __init__(self, store: Store, args: argparse.Namespace):
        self.store, self.args = store, args
        self.stats = Counter()
        self.next_admission = 0.0
        self.total = sum(store.counts().values())
        self.pending = store.counts().get("pending", 0)

    def add(self, domain: str, *, rate_limit: bool = True) -> None:
        if self.store.exists(domain):
            self.stats["duplicate"] += 1
            return
        clock = asyncio.get_running_loop().time()
        if (self.pending >= self.args.queue_size or self.total >= self.args.max_domains
                or (rate_limit and clock < self.next_admission)):
            self.stats["dropped_capacity_or_rate"] += 1
            return
        if self.store.admit(domain):
            self.total += 1
            self.pending += 1
            self.stats["admitted"] += 1
            self.next_admission = clock + 60 / self.args.max_per_minute


async def consume_feed(args, admission: Admission, stop: asyncio.Event) -> None:
    delay = 1.0
    while not stop.is_set():
        opened = asyncio.get_running_loop().time()
        try:
            # Bound both message bytes and buffered frames; no proxy env use.
            async with connect(args.feed_url, proxy=None, max_size=2_000_000,
                               max_queue=16, open_timeout=10, close_timeout=2,
                               ping_interval=20, ping_timeout=20) as websocket:
                LOG.info("CT feed connected")
                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=1)
                    except TimeoutError:
                        continue
                    try:
                        message = json.loads(raw)
                    except (ValueError, TypeError, RecursionError):
                        admission.stats["malformed"] += 1
                        continue
                    for domain in certificate_domains(message):
                        admission.add(domain)
                    # Give workers time even when recv() drains buffered frames.
                    await asyncio.sleep(0)
        except (OSError, WebSocketException, TimeoutError) as exc:
            LOG.warning("CT feed interrupted: %s; reconnecting", type(exc).__name__)
        if asyncio.get_running_loop().time() - opened > 60:
            delay = 1
        try:
            await asyncio.wait_for(stop.wait(), delay + random.uniform(0, 1))
        except TimeoutError:
            pass
        delay = min(delay * 2, 60)


async def run(args) -> None:
    store = Store(args.db, args.output)
    fetcher = Fetcher(args.concurrency, user_agent=args.user_agent)
    stop, finished = asyncio.Event(), asyncio.Event()
    admission = Admission(store, args)
    loop = asyncio.get_running_loop()
    installed_signals = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
            installed_signals.append(sig)
        except (NotImplementedError, RuntimeError):
            pass
    timer = loop.call_later(args.run_seconds, stop.set) if args.run_seconds else None

    async def producer():
        try:
            if args.domains_file:
                with args.domains_file.open(encoding="utf-8") as file:
                    for line in file:
                        if stop.is_set():
                            break
                        domain = apex_domain(line.strip())
                        if domain:
                            # Explicit batches wait for queue space instead of
                            # sampling, but still respect the total database cap.
                            while admission.pending >= args.queue_size and not stop.is_set():
                                await asyncio.sleep(0.1)
                            if not stop.is_set():
                                admission.add(domain, rate_limit=False)
                        await asyncio.sleep(0)
            else:
                await consume_feed(args, admission, stop)
        finally:
            finished.set()

    async def worker():
        while True:
            domain = store.claim()
            if domain is None:
                if finished.is_set():
                    return
                await asyncio.sleep(0.1)
                continue
            admission.pending -= 1
            try:
                page = await fetcher.fetch(domain)
                # Bounded by worker count; HTML parsing doesn't block feed I/O.
                result = await asyncio.to_thread(analyze, page.html, args.threshold)
            except Rejected as exc:
                store.fail(domain, "rejected", str(exc))
            except DNSFailure as exc:
                store.fail(domain, "dns_failed", str(exc))
            except (httpx.HTTPError, httpx.InvalidURL, TimeoutError, ValueError) as exc:
                store.fail(domain, "fetch_failed", type(exc).__name__)
            else:
                # Storage/export failures intentionally escape and fail fast;
                # do not relabel successfully scored pages as network failures.
                store.finish(domain, page.url, result)
                LOG.info("scored domain=%s status=%s score=%d", domain, result.status, result.score)
            admission.stats["processed"] += 1

    async def metrics():
        while not finished.is_set():
            try:
                await asyncio.wait_for(finished.wait(), timeout=30)
            except TimeoutError:
                LOG.info("stats=%s pending=%d", dict(admission.stats), admission.pending)

    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(producer())
            group.create_task(metrics())
            for _ in range(args.concurrency):
                group.create_task(worker())
    finally:
        if timer:
            timer.cancel()
        for sig in installed_signals:
            loop.remove_signal_handler(sig)
        await fetcher.aclose()
        LOG.info("final stats=%s statuses=%s", dict(admission.stats), store.counts())
        store.close()


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--feed-url", default="ws://127.0.0.1:8080/",
                   help="Certstream-compatible WebSocket endpoint")
    p.add_argument("--domains-file", type=Path, help="batch input, one hostname per line; skips CT")
    p.add_argument("--db", type=Path, default=Path("data/radar.sqlite3"))
    p.add_argument("--output", type=Path, default=Path("data/shortlists"))
    p.add_argument("--concurrency", type=int, default=10)
    p.add_argument("--queue-size", type=int, default=1000)
    p.add_argument("--max-per-minute", type=int, default=60,
                   help="maximum CT admissions/minute, spaced evenly; excess is sampled out")
    p.add_argument("--max-domains", type=int, default=100000,
                   help="total database row cap; existing pending work still runs")
    p.add_argument("--threshold", type=int, default=7, help="candidate score must be strictly greater")
    p.add_argument("--run-seconds", type=float, default=0, help="stop ingestion after N seconds, then drain")
    p.add_argument("--user-agent", default="StartupRadar/0.1 (startup discovery research)")
    return p


def main() -> None:
    p = parser()
    args = p.parse_args()
    for name in ("concurrency", "queue_size", "max_per_minute", "max_domains"):
        if getattr(args, name) < 1:
            p.error(f"{name} must be positive")
    if args.threshold < 0 or args.run_seconds < 0:
        p.error("threshold and run-seconds must be nonnegative")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    args.db = args.db.resolve()
    args.output = args.output.resolve()
    args.db.parent.mkdir(parents=True, exist_ok=True)
    args.output.mkdir(parents=True, exist_ok=True)
    try:
        # Serialize recovery and CSV appends, including separate DBs sharing output.
        with FileLock(str(args.db) + ".lock", timeout=0), FileLock(str(args.output / ".writer.lock"), timeout=0):
            asyncio.run(run(args))
    except LockTimeout:
        p.exit(1, "Another process owns the database or shortlist directory.\n")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
