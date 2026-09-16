"""Public-address-only HTTP fetching with DNS pinning, verified TLS, and limits."""
import asyncio
import ipaddress
from dataclasses import dataclass

import dns.asyncresolver
import dns.exception
import dns.resolver
import httpx

MAX_BODY = 500_000  # decimal KB; applies to raw, uncompressed HTML
REDIRECTS = {301, 302, 303, 307, 308}


class Rejected(Exception):
    """Policy rejection: must not trigger an HTTP downgrade."""


class DNSFailure(Exception):
    """No usable public DNS answer or DNS lookup failed."""


def public_ip(value: str) -> bool:
    try:
        addr = ipaddress.ip_address(value)
    except ValueError:
        return False
    # Block scope identifiers and IPv6 translation/tunnelling mechanisms too.
    return ("%" not in value and addr.is_global and not addr.is_multicast
            and not addr.is_reserved and not addr.is_unspecified
            and not (isinstance(addr, ipaddress.IPv6Address)
                     and (addr.ipv4_mapped or addr.sixtofour or addr.teredo
                          or addr in ipaddress.ip_network("64:ff9b::/96")
                          or addr in ipaddress.ip_network("64:ff9b:1::/48"))))


class PublicDNS:
    def __init__(self, timeout: float = 2.0):
        self.timeout = timeout
        self.resolver = dns.asyncresolver.Resolver()

    async def resolve(self, host: str) -> list[str]:
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            if not public_ip(host):
                raise Rejected("non-public IP")
            return [host]

        async def query(kind: str) -> list[str]:
            try:
                # dnspython follows CNAME chains for A/AAAA resolution.
                answer = await self.resolver.resolve(host + ".", kind,
                                                     lifetime=self.timeout, search=False)
                return [str(record) for record in answer]
            except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
                return []
            except dns.exception.DNSException as exc:
                raise DNSFailure(type(exc).__name__) from exc

        answers = await asyncio.gather(query("A"), query("AAAA"), return_exceptions=True)
        addresses = list(dict.fromkeys(ip for a in answers if isinstance(a, list) for ip in a))
        if any(not public_ip(ip) for ip in addresses):
            raise Rejected("DNS includes non-public address")
        # Fail closed if either family could not be checked (including timeout).
        for answer in answers:
            if isinstance(answer, BaseException):
                raise answer
        if not addresses:
            raise DNSFailure("no A/AAAA answer")
        return addresses


def validate_url(url: httpx.URL) -> None:
    if (url.scheme not in {"http", "https"} or not url.host or url.userinfo
            or url.port not in {None, 80, 443} or len(str(url)) > 4096):
        raise Rejected("unsupported URL, credentials, or port")


class PinnedTransport(httpx.AsyncBaseTransport):
    """Change socket destination, preserve HTTP Host and TLS server identity.

    No keepalive: separate hostnames sharing an IP must never reuse a TLS session
    established for a different hostname after URL rewriting.
    """
    def __init__(self, resolver: PublicDNS, concurrency: int = 10,
                 inner: httpx.AsyncBaseTransport | None = None):
        self.resolver = resolver
        self.inner = inner if inner is not None else httpx.AsyncHTTPTransport(
            verify=True, trust_env=False, retries=0,
            limits=httpx.Limits(max_connections=concurrency, max_keepalive_connections=0))

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        validate_url(request.url)
        addresses = await self.resolver.resolve(request.url.host)
        for index, address in enumerate(addresses):
            headers = request.headers.copy()
            headers["Host"] = request.url.netloc.decode("ascii")
            headers["Connection"] = "close"
            pinned = httpx.Request(
                request.method, request.url.copy_with(host=address), headers=headers,
                stream=request.stream,
                extensions={**request.extensions, "sni_hostname": request.url.host})
            try:
                return await self.inner.handle_async_request(pinned)
            except (httpx.ConnectError, httpx.ConnectTimeout):
                if index == len(addresses) - 1:
                    raise
        raise DNSFailure("empty address list")

    async def aclose(self) -> None:
        await self.inner.aclose()


@dataclass
class Page:
    url: str
    html: bytes


class Fetcher:
    def __init__(self, concurrency: int = 10, timeout: float = 5.0,
                 user_agent: str = "StartupRadar/0.1 (startup discovery research)",
                 transport: httpx.AsyncBaseTransport | None = None):
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(concurrency)
        self.client = httpx.AsyncClient(
            transport=transport if transport is not None else PinnedTransport(PublicDNS(), concurrency),
            timeout=httpx.Timeout(timeout), trust_env=False, follow_redirects=False,
            headers={"User-Agent": user_agent, "Accept": "text/html,application/xhtml+xml",
                     "Accept-Encoding": "identity"})

    async def _chain(self, url: str) -> Page:
        async with asyncio.timeout(self.timeout):
            current = httpx.URL(url)
            for hop in range(3):
                validate_url(current)
                async with self.client.stream("GET", current) as response:
                    self.client.cookies.clear()  # no cross-fetch cookie accumulation
                    if response.status_code in REDIRECTS:
                        location = response.headers.get("location")
                        if not location or hop == 2:
                            raise Rejected("missing Location or redirect limit")
                        current = current.join(location)
                        continue
                    response.raise_for_status()
                    content_type = response.headers.get("content-type", "").split(";")[0].lower().strip()
                    if content_type not in {"text/html", "application/xhtml+xml"}:
                        raise Rejected("non-HTML content")
                    if response.headers.get("content-encoding", "identity").lower().strip() != "identity":
                        # Never decompress attacker-controlled data; avoid zip bombs.
                        raise Rejected("compressed response despite identity request")
                    size = response.headers.get("content-length")
                    if size is not None:
                        try:
                            if int(size) < 0 or int(size) > MAX_BODY:
                                raise Rejected("body size limit")
                        except ValueError as exc:
                            raise Rejected("invalid Content-Length") from exc
                    body = bytearray()
                    async for chunk in response.aiter_raw():
                        if len(body) + len(chunk) > MAX_BODY:
                            raise Rejected("body size limit")
                        body.extend(chunk)
                    return Page(str(current), bytes(body))
        raise Rejected("redirect limit")

    async def fetch(self, domain: str) -> Page:
        async with self.semaphore:
            try:
                return await self._chain(f"https://{domain}/")
            except (httpx.TransportError, TimeoutError):
                # Only connectivity/TLS failures trigger fallback, never SSRF,
                # HTTP status, DNS policy, content policy, or redirect rejection.
                return await self._chain(f"http://{domain}/")

    async def aclose(self) -> None:
        await self.client.aclose()
