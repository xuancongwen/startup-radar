import asyncio
import csv
import json

import dns.resolver
import httpx
import pytest

from startup_radar.domains import apex_domain, certificate_domains
from startup_radar.fetch import Fetcher, MAX_BODY, PinnedTransport, PublicDNS, Rejected, public_ip
from startup_radar.pipeline import parser, run
from startup_radar.scoring import analyze
from startup_radar.storage import Store


@pytest.mark.parametrize("name,expected", [
    ("www.Example.com", "example.com"), ("foo.example.ai.", "example.ai"),
    ("*.example.com", None), ("cpanel.example.com", None),
    ("foo.autodiscover.example.io", None), ("example.org", None),
    ("foo.github.io", None), ("https://example.com", None), ("127.0.0.1", None),
    ("-bad.com", None), ("例子.com", "xn--fsqu00a.com"), (None, None),
])
def test_apex(name, expected):
    assert apex_domain(name) == expected


def test_feed_shape():
    assert certificate_domains({"message_type": "heartbeat"}) == []
    assert certificate_domains({"message_type": "certificate_update", "data": None}) == []
    message = {"message_type": "certificate_update", "data": {"leaf_cert": {
        "all_domains": ["example.com", "www.example.com", 3, "*.example.io"]}}}
    assert certificate_domains(message) == ["example.com"]


@pytest.mark.parametrize("ip", ["127.0.0.1", "10.1.2.3", "172.16.1.2", "192.168.1.1",
                                "169.254.169.254", "100.64.0.1", "0.0.0.0", "224.0.0.1",
                                "::1", "fc00::1", "fe80::1", "::ffff:127.0.0.1",
                                "64:ff9b::7f00:1", "2002:7f00:1::", "192.0.2.1"])
def test_nonpublic(ip):
    assert not public_ip(ip)


def test_public():
    assert public_ip("8.8.8.8")
    assert public_ip("2606:4700:4700::1111")


async def test_dns_mixed_answer_rejected(monkeypatch):
    resolver = PublicDNS()
    async def resolve(host, kind, **kwargs):
        return ["8.8.8.8"] if kind == "A" else ["::1"]
    monkeypatch.setattr(resolver.resolver, "resolve", resolve)
    with pytest.raises(Rejected):
        await resolver.resolve("example.com")


async def test_dns_terminal_answer(monkeypatch):
    resolver = PublicDNS()
    async def resolve(host, kind, **kwargs):
        assert host == "example.com." and kwargs["search"] is False
        if kind == "AAAA":
            raise dns.resolver.NoAnswer
        return ["8.8.8.8"]
    monkeypatch.setattr(resolver.resolver, "resolve", resolve)
    assert await resolver.resolve("example.com") == ["8.8.8.8"]


class FakeDNS:
    def __init__(self):
        self.calls = []
    async def resolve(self, host):
        self.calls.append(host)
        if host != "example.com":
            raise Rejected("blocked redirect target")
        return ["8.8.8.8"]


class Raw(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk


def response(body=b"<title>Example</title>", **kwargs):
    return httpx.Response(200, headers={"content-type": "text/html", **kwargs}, stream=Raw([body]))


async def test_pinning_host_sni_and_redirect_ssrf():
    dns = FakeDNS()
    requests = []
    def handler(request):
        requests.append(request)
        assert request.url.host == "8.8.8.8"
        assert request.headers["host"] == "example.com"
        assert request.extensions["sni_hostname"] == "example.com"
        return httpx.Response(302, headers={"location": "http://169.254.169.254/latest"})
    fetcher = Fetcher(transport=PinnedTransport(dns, inner=httpx.MockTransport(handler)))
    try:
        with pytest.raises(Rejected):
            await fetcher.fetch("example.com")
        assert len(requests) == 1
        assert dns.calls == ["example.com", "169.254.169.254"]
    finally:
        await fetcher.aclose()


@pytest.mark.parametrize("target", ["file:///etc/passwd", "http://user:pass@example.com",
                                    "https://example.com:8080/"])
async def test_bad_redirect_url(target):
    fetcher = Fetcher(transport=httpx.MockTransport(
        lambda request: httpx.Response(302, headers={"location": target})))
    try:
        with pytest.raises(Rejected):
            await fetcher.fetch("example.com")
    finally:
        await fetcher.aclose()


async def test_two_redirects_allowed_three_rejected():
    count = 0
    def handler(request):
        nonlocal count
        count += 1
        return httpx.Response(302, headers={"location": "/next"}) if count < 3 else response()
    fetcher = Fetcher(transport=httpx.MockTransport(handler))
    try:
        assert (await fetcher.fetch("example.com")).url == "https://example.com/next"
        assert count == 3
    finally:
        await fetcher.aclose()
    fetcher = Fetcher(transport=httpx.MockTransport(
        lambda request: httpx.Response(302, headers={"location": "/again"})))
    try:
        with pytest.raises(Rejected, match="redirect limit"):
            await fetcher.fetch("example.com")
    finally:
        await fetcher.aclose()


@pytest.mark.parametrize("body,headers", [
    (b"x" * (MAX_BODY + 1), {}), (b"x", {"content-length": str(MAX_BODY + 1)}),
    (b"x", {"content-encoding": "gzip"}), (b"x", {"content-type": "application/pdf"}),
])
async def test_response_limits(body, headers):
    fetcher = Fetcher(transport=httpx.MockTransport(lambda request: response(body, **headers)))
    try:
        with pytest.raises(Rejected):
            await fetcher.fetch("example.com")
    finally:
        await fetcher.aclose()


async def test_cumulative_timeout_and_fallback():
    schemes = []
    async def handler(request):
        schemes.append(request.url.scheme)
        if request.url.scheme == "https":
            await asyncio.sleep(0.1)
        return response()
    fetcher = Fetcher(timeout=0.02, transport=httpx.MockTransport(handler))
    try:
        assert (await fetcher.fetch("example.com")).url == "http://example.com/"
        assert schemes == ["https", "http"]
    finally:
        await fetcher.aclose()


async def test_semaphore():
    active = peak = 0
    async def handler(request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return response()
    fetcher = Fetcher(concurrency=2, transport=httpx.MockTransport(handler))
    try:
        await asyncio.gather(*(fetcher.fetch("example.com") for _ in range(8)))
        assert peak == 2
    finally:
        await fetcher.aclose()


def test_scoring_extraction_and_boundaries():
    result = analyze(b'''<title>Tool</title><meta name="description" content="Private beta">
        <meta property="og:title" content="AI platform"><script>domain for sale</script>
        <div hidden>godaddy</div><style>sedo</style><p>API API API automation</p>''', 7)
    assert result.status == "startup_candidate" and result.score == 13
    assert result.og_tags == {"og:title": "AI platform"}
    assert result.description == "Private beta"
    assert "godaddy" not in result.visible_text
    assert analyze(b"capital rapid workflows", 7).score == 0
    assert analyze(b"early access automation", 7).status == "live"


@pytest.mark.parametrize("phrase", ["domain for sale", "buy this domain", "parked free",
                                    "under construction", "hugedomains", "sedo", "godaddy",
                                    "namecheap parking"])
def test_parked_precedence(phrase):
    result = analyze((phrase + " AI platform private beta API").encode(), 7)
    assert result.status == "parked" and result.score == 0


def test_sqlite_dedup_recovery_and_csv(tmp_path):
    db, output = tmp_path / "test.sqlite3", tmp_path / "csv"
    store = Store(db, output)
    assert store.admit("example.com")
    assert not store.admit("example.com")
    assert store.claim() == "example.com"
    store.close()
    store = Store(db, output)
    assert store.claim() == "example.com"
    result = analyze(b"<title>=BAD()</title>private beta AI platform API", 7)
    store.finish("example.com", "https://example.com/", result)
    csv_path = next(output.glob("shortlist_*.csv"))
    with csv_path.open("a") as file:
        file.write("partial crash")
    store.close()
    store = Store(db, output)
    rows = list(csv.DictReader(csv_path.open()))
    assert len(rows) == 1 and rows[0]["title"] == "'=BAD()"
    assert json.loads(rows[0]["matched_signals"])
    assert store.claim() is None
    store.close()


async def test_pipeline_batch_end_to_end(tmp_path, monkeypatch):
    from startup_radar import pipeline
    real_fetcher = Fetcher
    def make_fetcher(*args, **kwargs):
        return real_fetcher(transport=httpx.MockTransport(
            lambda request: response(b"AI platform private beta API")))
    monkeypatch.setattr(pipeline, "Fetcher", make_fetcher)
    source = tmp_path / "domains.txt"
    source.write_text("example.com\nwww.example.com\n*.ignore.com\nother.ai\n")
    args = parser().parse_args(["--domains-file", str(source), "--db", str(tmp_path / "db"),
                               "--output", str(tmp_path / "csv"), "--queue-size", "1"])
    await run(args)
    await run(args)
    rows = list(csv.DictReader(next((tmp_path / "csv").glob("*.csv")).open()))
    assert {r["domain"] for r in rows} == {"example.com", "other.ai"}
    assert len(rows) == 2


async def test_real_websocket_ingestion(tmp_path):
    from websockets.asyncio.server import serve
    from startup_radar.pipeline import Admission, consume_feed
    stop = asyncio.Event()
    async def handler(socket):
        await socket.send("{bad json")
        await socket.send(json.dumps({"message_type": "heartbeat"}))
        await socket.send(json.dumps({"message_type": "certificate_update", "data": {
            "leaf_cert": {"all_domains": ["newco.ai", "www.newco.ai"]}}}))
        await stop.wait()
    store = Store(tmp_path / "db", tmp_path / "csv")
    try:
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            args = parser().parse_args(["--feed-url", f"ws://127.0.0.1:{port}"])
            admission = Admission(store, args)
            task = asyncio.create_task(consume_feed(args, admission, stop))
            try:
                async with asyncio.timeout(3):
                    while not store.exists("newco.ai"):
                        await asyncio.sleep(0.01)
            finally:
                stop.set()
                await task
            assert admission.stats["admitted"] == 1
            assert admission.stats["malformed"] == 1
    finally:
        store.close()


async def test_real_tls_preserves_hostname(tmp_path, monkeypatch):
    """Exercise real TLS over a local fixture; only test code reroutes sockets."""
    import shutil
    import ssl
    import subprocess
    from httpcore._backends.anyio import AnyIOBackend
    if not shutil.which("openssl"):
        pytest.skip("openssl required for TLS fixture")
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                    "-keyout", str(key), "-out", str(cert), "-days", "1",
                    "-subj", "/CN=example.com", "-addext", "subjectAltName=DNS:example.com"],
                   check=True, capture_output=True)
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(cert, key)
    names = []
    server_context.set_servername_callback(lambda sock, name, ctx: names.append(name))
    received = []
    async def serve_http(reader, writer):
        try:
            received.append(await reader.readuntil(b"\r\n\r\n"))
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
    server = await asyncio.start_server(serve_http, "127.0.0.1", 0, ssl=server_context)
    port = server.sockets[0].getsockname()[1]
    original_connect = AnyIOBackend.connect_tcp
    async def reroute(self, host, port=None, **kwargs):
        assert host == "8.8.8.8"  # production transport passes a vetted literal
        return await original_connect(self, "127.0.0.1", server.sockets[0].getsockname()[1], **kwargs)
    monkeypatch.setattr(AnyIOBackend, "connect_tcp", reroute)
    context = ssl.create_default_context(cafile=str(cert))
    inner = httpx.AsyncHTTPTransport(verify=context, trust_env=False,
                                     limits=httpx.Limits(max_keepalive_connections=0))
    dns = FakeDNS()
    fetcher = Fetcher(transport=PinnedTransport(dns, inner=inner))
    try:
        page = await fetcher.fetch("example.com")
        assert page.html == b"OK"
        assert names == ["example.com"]
        assert b"Host: example.com" in received[0]
        # A trusted certificate for the wrong hostname still fails verification.
        async def any_host(host):
            return ["8.8.8.8"]
        monkeypatch.setattr(dns, "resolve", any_host)
        with pytest.raises(httpx.ConnectError):
            await fetcher._chain("https://wrong.com/")
    finally:
        await fetcher.aclose()
        server.close()
        await server.wait_closed()
