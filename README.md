# Startup Radar

A self-contained Python 3.11+ service that samples newly issued TLS certificates,
checks public homepages, scores them with explainable phrase rules, and serves the
resulting startup candidates over a small read-only HTTP API for other systems to
poll. No paid API, LLM, browser, Redis, or external database is required. It runs
as three processes on one box: a Certificate Transparency aggregator, the pipeline,
and the API. Deployment targets: a DigitalOcean droplet, a homelab Linux server, or
a Proxmox LXC container (see [Deployment](#deployment)).

**CT issuance is not domain registration, company formation, or proof of being
unfunded.** Renewal certificates and established companies appear too. `first_seen_at`
means first admitted by this database. Scores prioritize manual review; they do not
verify funding status, business legitimacy, or investment suitability.

## Quick start

The fastest path on any host with Docker is Compose, which starts all three services:

```bash
cp .env.example .env          # set RADAR_TOKENS (openssl rand -hex 32) and a contact address
docker compose up -d --build
curl http://127.0.0.1:8081/health
TOKEN=$(sed -n 's/^RADAR_TOKENS=[^:]*://p' .env)
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8081/stats
```

The API listens on localhost only by default. Candidates begin appearing within a few
minutes at the default intake of 20 domains per minute. See [Deployment](#deployment)
for exposing the API to other machines, and [HTTP API](#http-api) for the contract.
Without Docker, `deploy/install.sh` sets up the same three services under systemd.

### Feed requirement

Startup Radar needs a Certstream-compatible WebSocket feed of newly issued
certificates. **The original public feed at `certstream.calidog.io` no longer
delivers data**: it accepts WebSocket connections but sends no messages (verified
2026-09-14 across `/`, `/full-stream`, and `/domains-only` for 60 seconds each;
users have reported the same since March 2025). Its maintainer has said the public
server was only ever meant for simple proofs of concept and that anyone ingesting
the data should run their own server. Do not build on it.

Both deployment paths therefore run `certstream-server-go`, a drop-in replacement
that reads the public CT logs directly and streams the same JSON shape. The configs
under `deploy/` bind it so that only the pipeline can reach it. For development on a
host without the compose stack, start it by hand:

```bash
docker run -d --name certstream --restart unless-stopped \
  -p 127.0.0.1:8080:8080 0rickyy0/certstream-server-go:v1.10.1
```

It begins streaming within about 30 seconds. Check it with `docker logs certstream`;
you should see `Processed N entries` lines climbing. The default `--feed-url` is
`ws://127.0.0.1:8080/`, so no flag is needed once the container is up.

### Development

Install and run the pipeline directly from the repository (the compose stack must be
stopped first, since only one process may own the database):

```bash
python3 -m venv .venv
source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -e '.[test]'
python -m pytest -q
python -m startup_radar.pipeline --run-seconds 300 --max-per-minute 10
```

For a continuous run:

```bash
startup-radar --concurrency 10 --threshold 7 \
  --max-per-minute 60 \
  --user-agent 'StartupRadar/0.1 (research; contact: YOUR_EMAIL)'
```

Replace `YOUR_EMAIL` with your actual contact if using that example. Ctrl+C or
SIGTERM stops intake and drains admitted work. `--run-seconds` limits intake time,
not total drain time. Restarting requeues interrupted work automatically.

If the feed cannot be reached, the process logs reconnect attempts with exponential
backoff and jitter and processes nothing. A log line reading `CT feed connected`
followed by `stats={}` every 30 seconds means the socket is open but silent, which
is what the dead public feed looks like. To use a feed on another host, override
the endpoint:

```bash
startup-radar --feed-url ws://feed.internal:8080/ --run-seconds 300
```

The feed is trusted operator configuration; local and private-network feed
connections are allowed. Homepage targets from certificates always go through the
SSRF checks. Expected feed shape:

```json
{"message_type":"certificate_update","data":{"leaf_cert":{"all_domains":["newco.ai","www.newco.ai"]}}}
```

Heartbeat and malformed messages are ignored. This uses `websockets` directly to
keep ingestion asynchronous rather than the synchronous certstream callback client.

## Directory layout

```text
startup-radar/
  README.md
  pyproject.toml
  Dockerfile              # one image: pipeline (default command) or API
  docker-compose.yml      # certstream + pipeline + API, API on localhost
  docker-compose.tls.yml  # overlay: Caddy with automatic TLS for a public domain
  Caddyfile
  .env.example            # tokens, intake limits, bind address
  startup_radar/
    __init__.py
    domains.py        # offline public suffix parsing, SAN filtering
    fetch.py          # asynchronous DNS, pinned transport, HTTP limits
    scoring.py        # metadata/text extraction and weighted phrase rules
    storage.py        # SQLite queue, results, daily CSV recovery
    pipeline.py       # CLI, feed reconnection, admission controls, workers
    serve.py          # read-only HTTP API over the SQLite file
  deploy/
    install.sh              # no-Docker install: systemd units, venv, certstream binary
    env.example             # /etc/startup-radar/env template
    certstream.docker.yaml  # aggregator config for the compose network
    certstream.systemd.yaml # aggregator config bound to loopback
    systemd/*.service       # certstream, startup-radar, startup-radar-api
  tests/
    test_radar.py
    test_serve.py
```

Runtime files are created under `data/` and excluded from git:
`radar.sqlite3` plus SQLite WAL files, and `shortlists/shortlist_YYYYMMDD.csv`.

## Processing behavior

1. Normalize SANs to lowercase IDNA. Allow `.com`, `.ai`, `.io`, `.co`, `.dev`,
   `.app`, `.xyz`. Extract registrable apex domains using the bundled public suffix
   snapshot; no list download occurs at runtime. Ignore wildcard SAN entries,
   internal labels such as `cpanel`/`autodiscover`, invalid hostnames, and private
   hosting suffixes such as tenant `github.io` domains. `www.example.com` becomes
   `example.com`; `*.example.com` alone does not create a discovery.
2. Deduplicate using SQLite's primary key. Admission is spaced evenly and bounded
   by pending queue size and the total database row cap. Accepted discoveries are
   committed before processing, so they survive restarts.
3. Workers resolve A and AAAA concurrently with a two-second DNS lifetime.
   dnspython follows CNAME chains to their terminal address records; a bare CNAME
   with no usable address is not enough. Empty resolution becomes `dns_failed`.
4. Fetch HTTPS first. A connectivity/TLS timeout or transport failure permits an
   HTTP fallback. HTTP 4xx/5xx, DNS rejection, prohibited URLs, oversize content,
   and other policy failures do not trigger fallback.
5. Follow at most two redirects per attempt, resolving and validating each target.
   Each complete scheme attempt has a **five-second wall-clock budget**, including
   DNS, redirects, and streaming, plus HTTPX's five-second phase timeouts. HTTPS
   plus HTTP fallback can therefore take up to about ten seconds, excluding queue
   wait and parsing/storage. Response cleanup can add small scheduling overhead.
6. Accept successful HTML/XHTML responses only. Stream a maximum **500,000 bytes**;
   reject oversized bodies even without `Content-Length`. Request identity
   encoding and reject servers that send compressed content anyway, avoiding
   decompression bombs. No JavaScript, images, stylesheets, or linked pages load.
7. Extract title, description, Open Graph tags, and approximate visible text with
   BeautifulSoup. Strip scripts/styles/templates and common inline hidden content.
   Score title, description, OG values, and text. Full body text is used transiently
   and not retained, keeping database storage smaller.
8. Commit results and append candidates to a daily UTC CSV. Parked pages stay in
   SQLite for deduplication and audit, but never enter the shortlist.

## Scoring

Every distinct phrase scores once, regardless of repetition or where it appears.
Matching is case-insensitive with word boundaries and normalized whitespace.

| Category | Points per phrase | Phrases |
| --- | ---: | --- |
| Founder/hiring | 2 | we are hiring; join our team; careers; about us; founder |
| Early stage | 4 | join waitlist; join the waitlist; private beta; early access; coming soon; backed by; launching in |
| Tech/product | 3 | AI platform; API; developer tool; automation; workflow; infrastructure; SaaS |

The default threshold is 7: **score > 7** becomes `startup_candidate`; a score of
7 remains `live`. For example, “private beta” + “AI platform” + “API” scores 10.
Change `--threshold` or edit `RULES` in `scoring.py` to tune precision/recall.

These disqualifiers override all positive scores: `domain for sale`,
`buy this domain`, `parked free`, `under construction`, `hugedomains`, `sedo`,
`godaddy`, `namecheap parking`. They set `parked` and score zero. Generic “coming
soon” can be a startup signal; “under construction” is a placeholder disqualifier.
Brand-word disqualifiers can reject legitimate pages mentioning those brands;
this deliberately implements the requested conservative rules.

## Storage and export

| Column | Meaning |
| --- | --- |
| domain | Apex domain, primary key |
| first_seen_at | UTC time first admitted by this instance |
| resolved_url | Final logical HTTP URL, preserving hostname rather than pinned IP |
| title / description | Extracted metadata, capped at 1,000 / 2,000 characters |
| score | Sum of unique matched phrases, zero for parked pages |
| matched_signals | JSON array with category, phrase, and points |
| status | parked, live, startup_candidate; operational states below |
| og_tags | Extracted Open Graph values as JSON |
| checked_at | Last completed processing timestamp in UTC |
| shortlist_day | UTC qualification date YYYYMMDD, or NULL |
| error | Bounded processing error/rejection reason |

Operational states are `pending`, `processing`, `dns_failed`, `fetch_failed`, and
`rejected`. Failed attempts are retained instead of being mislabeled as live.
SQLite uses parameterized queries, WAL, and synchronous FULL commits. Synchronous
local disk operations are intentionally short and serialized in the event loop;
HTML parsing runs in bounded worker threads. This is a single-process local-disk
PoC, not a high-throughput database design. Do not put its SQLite file on NFS.

Candidates append immediately to `shortlist_YYYYMMDD.csv`, using qualification day
rather than CT issuance day. CSV files contain the eight requested core columns.
Text cells that could execute spreadsheet formulas are escaped. File locks prevent
multiple writers sharing a database or export directory.

SQLite is authoritative. On startup, each candidate day's CSV is atomically rebuilt
from stored rows to repair missing, partial, or duplicate appends after a crash.
A disk/export failure stops the pipeline; it does not discard the committed result.
Use a dedicated output directory and do not manually edit generated shortlists.
Changing the threshold does not retroactively rescore stored results.

Inspect results with SQLite tooling, for example:

```sql
SELECT domain, score, title, resolved_url, matched_signals
FROM domains WHERE status = 'startup_candidate'
ORDER BY score DESC, first_seen_at DESC;
```

To retry transient failures, stop the process and explicitly requeue them:

```sql
UPDATE domains SET status = 'pending', error = NULL
WHERE status IN ('dns_failed', 'fetch_failed');
```

First-seen timestamps remain intact. There is no automatic revisit schedule in this
PoC; freshly certified domains may not yet be serving content. A delayed revisit
policy is the most useful next extension for improving discovery recall.

## HTTP API

`startup-radar-serve` is a second process over the same SQLite file. It opens the
database read-only on every request, never takes the pipeline's lock, and uses only
the standard library. Clients see startup candidates and nothing else: pending, live,
parked, rejected, and failed rows are not served.

| Route | Auth | Purpose |
| --- | --- | --- |
| `GET /` | none | Route listing |
| `GET /health` | none | `{"status":"ok","last_checked_at":...}`; 503 until the database exists |
| `GET /status` | none | Human status page: pipeline freshness, 24-hour counts, candidates per day, rows by status |
| `GET /stats` | bearer | Row counts by status, total, last check and last candidate timestamps |
| `GET /candidates` | bearer | Candidate rows, oldest first, with an opaque cursor |

`/candidates` parameters:

| Parameter | Default | Meaning |
| --- | --- | --- |
| `cursor` | none | Opaque position from the previous response's `next_cursor` |
| `limit` | 100 | Rows per page, capped at 1,000 |
| `min_score` | server floor | A client may raise the floor, never lower it below `RADAR_MIN_SCORE` |
| `day` | none | Only rows that qualified on this UTC day, `YYYYMMDD` |

A browser opening `/` is redirected to `/status`. The page needs no token and therefore
shows no domain names, only counts and timestamps. It refreshes itself every minute
and flags the pipeline as stale when nothing has been checked for ten minutes, which
is the quickest way to notice a dead feed. It carries a strict Content-Security-Policy
and runs no script.

Each row carries `domain`, `first_seen_at`, `checked_at`, `shortlist_day`,
`resolved_url`, `title`, `description`, `score`, `matched_signals` (the phrases that
hit, with category and points), and `og_tags`. Rows are ordered by `checked_at` then
`domain`, and the cursor encodes that pair, so paging is stable even when many rows
share a second. `next_cursor` is `null` on the last page. A client that stores the
last cursor it saw and polls with it receives each candidate exactly once.

Authentication is a bearer token. `RADAR_TOKENS` holds comma-separated `label:secret`
pairs, one per client; the label appears in the access log, the secret never does.
Secrets must be at least 16 characters and are compared in constant time. The server
refuses to start with no tokens unless `--allow-anonymous` is passed explicitly.
Responses are JSON with `Cache-Control: no-store`. Write methods return 405.

Example, as a MondayFlow instance or any other poller would run it:

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "https://radar.example.com/candidates?min_score=10&limit=200"
```

```js
// Poll once an hour; keep `cursor` somewhere durable between runs.
async function pullCandidates(base, token, cursor) {
  const out = [];
  for (;;) {
    const url = new URL('/candidates', base);
    url.searchParams.set('limit', '200');
    if (cursor) url.searchParams.set('cursor', cursor);
    const res = await fetch(url, { headers: { authorization: `Bearer ${token}` } });
    if (!res.ok) throw new Error(`radar ${res.status}`);
    const body = await res.json();
    out.push(...body.candidates);
    if (!body.next_cursor) return { candidates: out, cursor };
    cursor = body.next_cursor;
  }
}
```

The API is a data feed, not an agreement: consumers decide what to do with a row.
Nothing about a candidate is verified beyond what the scoring section describes.

## Deployment

All three targets run the same three services. Pick Compose where Docker is
available and the systemd path where it is not. Neither needs a public IP for the
API unless you want one.

### DigitalOcean droplet, or any host with a public DNS name

```bash
git clone <this repo> /opt/startup-radar && cd /opt/startup-radar
cp .env.example .env    # RADAR_TOKENS, a contact address, DOMAIN=radar.example.com
docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d --build
```

Caddy obtains the certificate for `DOMAIN` and proxies to the API; the API itself
stays bound to localhost. Open only 22, 80, and 443 in the cloud firewall. The
smallest 1 GB droplet is enough: measured at 20 admissions per minute, the aggregator
used 68 MB and a third of one core, the pipeline 49 MB, and the API 28 MB. Inbound CT
traffic of about 13.5 Mbit/s does not count toward DigitalOcean's outbound transfer
allowance.

### Homelab Linux server with Docker

Same as above without the overlay. Set `RADAR_BIND` in `.env` to the server's LAN
address, or `0.0.0.0` if your firewall already limits who can reach it, and point
clients at `http://<lan-ip>:8081/`. If clients cross the internet, terminate TLS on
whatever reverse proxy you already run and keep `RADAR_BIND=127.0.0.1`.

### Proxmox LXC, or any Linux host without Docker

Docker inside an unprivileged LXC container needs `nesting=1` and `keyctl=1` in the
container's features and is fragile across Proxmox upgrades. The systemd path avoids
it. In a Debian or Ubuntu container with `python3`, `python3-venv`, and `curl`:

```bash
git clone <this repo> /usr/local/src/startup-radar
sudo /usr/local/src/startup-radar/deploy/install.sh
```

The script is idempotent and runs as root. It creates the `radar` system user,
downloads the `certstream-server-go` release binary for the container's architecture
and verifies its SHA-256 against the published checksums, builds a virtualenv under
`/opt/startup-radar`, writes `/etc/startup-radar/env` with a generated token on
first run and leaves it alone afterwards, installs three hardened units, and enables
them. Data lives in `/var/lib/startup-radar`. Re-run it after a `git pull` to upgrade.
Set `RADAR_BIND` in the env file to expose the API beyond loopback, then
`systemctl restart startup-radar-api`.

Resource notes for an LXC: 512 MB of RAM and one core are enough for the measured
footprint above, with headroom;
the aggregator opens many outbound HTTPS connections, so raise the container's
`nofile` limit if the certstream log reports connection failures.

### Operating

- `docker compose logs -f pipeline` or `journalctl -u startup-radar -f` shows admissions,
  scoring, and the 30-second counters.
- Open `/status` in a browser for a glance at freshness and counts. The `/stats` route
  gives the same numbers as JSON; `last_checked_at` should advance every few seconds
  while the pipeline runs.
- Rotate a client's token by editing `RADAR_TOKENS` and restarting the API only; the
  pipeline is unaffected.
- Back up `radar.sqlite3` with `sqlite3 radar.sqlite3 ".backup out.sqlite3"` while
  running, or copy the file and its `-wal` companion together while stopped.

## Controls and cost

| Option | Default | Purpose |
| --- | --- | --- |
| --feed-url | ws://127.0.0.1:8080/ | Certstream-compatible WebSocket endpoint (see Quick start) |
| --concurrency | 10 | Fixed workers plus fetch semaphore |
| --queue-size | 1000 | Maximum newly admitted pending rows |
| --max-per-minute | 60 | CT admissions spaced one second apart by default |
| --max-domains | 100000 | Total retained rows before new admissions stop |
| --threshold | 7 | Strictly greater score qualifies |
| --run-seconds | 0 | Unlimited intake; positive value sets an intake deadline |
| --db | data/radar.sqlite3 | Durable state |
| --output | data/shortlists | UTC daily exports |

`startup-radar-serve` options:

| Option | Default | Purpose |
| --- | --- | --- |
| --db | data/radar.sqlite3 | Database to read |
| --host / --port | 127.0.0.1 / 8081 | Bind address; put TLS in front before leaving localhost |
| --tokens | $RADAR_TOKENS | `label:secret` pairs, comma-separated |
| --tokens-file | none | Same pairs, one per line |
| --allow-anonymous | off | Serve with no tokens; only behind a firewall you trust |
| --min-score | $RADAR_MIN_SCORE or 0 | Lowest score served; clients may raise it |
| --max-limit | 1000 | Cap on a page |

CT intake is **sampled**, not lossless: new domains arriving while admission is
rate-limited or full are dropped and counted in `dropped_capacity_or_rate` logs.
Dropped names are not recorded as processed and may be admitted if seen again.
Counters log every 30 seconds and at shutdown. No replay cursor is available, so
reconnects and downtime also create gaps. The 100,000-row cap stops new discovery
until you increase it or explicitly archive state; existing pending work still runs.

Filtering reduces homepage traffic, not the bandwidth of the incoming full CT
feed. A local `certstream-server-go` instance emits roughly 2,000 certificates per
second and, per its author, pulls about 13.5 Mbit/s from the CT logs plus 1 to 20
Mbit/s per connected client. At 10 to 20 admissions per minute the pipeline
samples well under one percent of that stream; the rest is counted as dropped. Even 60 admitted domains/minute is up to 86,400/day: at the full body limit,
one successful homepage each would be 43.2 GB/day before CT/TLS overhead. Actual
volume depends on pages and failures. Start with the five-minute, 10/minute command
above, assess yield, and scale deliberately. No paid service is configured.

For a finite domain batch that skips CT:

```bash
# Create domains.txt with one hostname per line, then:
startup-radar --domains-file domains.txt --concurrency 5
```

Batch mode waits for queue capacity and bypasses CT admission rate sampling. It
still deduplicates, obeys the total row cap, and uses all DNS/HTTP safeguards.

## Security and limitations

The transport rejects non-public addresses including RFC1918, loopback, link-local,
CGNAT, multicast, reserved/documentation space, IPv6 ULA, and selected IPv6
translation/tunnel formats. Mixed public/private DNS answers are rejected. If either
DNS family fails unexpectedly, resolution fails closed.

A pre-flight check alone is insufficient against DNS rebinding. This implementation
connects HTTPX to the validated literal IP and preserves the logical `Host` header
and HTTPCore `sni_hostname` extension for TLS hostname validation. The socket layer
never resolves the untrusted hostname again. Keepalive is disabled to prevent TLS
connection reuse between distinct domains sharing an IP. Every redirect repeats
validation. Only HTTP(S), no URL credentials, and ports 80/443 are allowed; proxy
environment variables are ignored. TLS verification remains enabled. Cookies are
cleared after response headers to avoid accumulated cross-fetch state.

A page can supply misleading text and score highly. Static extraction cannot
reproduce computed CSS visibility or JavaScript-rendered content. English phrases
miss other languages. No funding database, registration-age check, robots.txt
scheduler, historical CT backfill, automatic retry schedule, or crawl frontier is
included. This fetches at most one redirect chain per scheme per admitted apex.

## Validation and references

71 tests passed on Python 3.12 and 3.14, including the API (auth, paging with tied
timestamps, filters, read-only opens, missing database, the status page) and mocked batch-to-SQLite-to-CSV execution,
restart recovery, address/redirect policies, body caps, timeout/fallback, semaphore
concurrency, a real local WebSocket feed, and real local TLS validating the correct
hostname and rejecting a wrong one. The TLS fixture needs the `openssl` executable
and skips that test if absent. Tests do not crawl public websites.

Live behavior was verified on 2026-09-14 on Linux x86_64 with Docker. A fresh
`0rickyy0/certstream-server-go` container delivered 22,319 messages in its first
10 seconds. A two-minute pipeline run against it at 20 admissions per minute and
concurrency 10 admitted 40 domains, dropped 87,587 by rate sampling, and finished
with 23 live, 2 startup_candidate, 2 parked, 2 rejected, 6 dns_failed, and 9
fetch_failed; both candidates were appended to that day's CSV. The same day, the
public `certstream.calidog.io` endpoint opened connections but delivered zero
messages over several minutes, so it is no longer the default.

On 2026-09-16 the compose stack was brought up from a clean build on Linux x86_64: the
pipeline connected to the aggregator container through the mounted config, new rows
appeared through `/stats` within a minute, and `/candidates` paged with a bearer
token while unauthenticated requests received 401. The TLS overlay was validated with
`docker compose config` but not brought up. `deploy/install.sh` was exercised in a
fresh `python:3.12-slim` container with `systemctl` stubbed: it downloaded and
checksum-verified the aggregator binary, built the virtualenv, generated a token,
and was idempotent on a second run. It has not yet been run on a real Proxmox LXC.

HTTPX 0.28.1 and HTTPCore 1.0.9 are pinned because the pinning transport depends on
their transport/extension behavior. Other dependencies have bounded version ranges
in `pyproject.toml`. Re-run the security/TLS tests when upgrading.

Primary implementation references:

- [Original Certstream server and message format](https://github.com/CaliDog/certstream-server)
- [Certstream-compatible Go server](https://github.com/d-Rickyy-b/certstream-server-go)
- [certstream-server-go Docker image](https://hub.docker.com/r/0rickyy0/certstream-server-go)
- [Public certstream-server down, maintainer response](https://github.com/CaliDog/certstream-server/issues/110)
- [HTTPX custom transports](https://www.python-httpx.org/advanced/transports/)
- [HTTPCore SNI hostname extension](https://www.encode.io/httpcore/extensions/#sni_hostname)
