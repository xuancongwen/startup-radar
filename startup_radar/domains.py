"""Offline public-suffix parsing and conservative certificate-name filtering."""
import re

import tldextract

TLD_ALLOWLIST = frozenset({"com", "ai", "io", "co", "dev", "app", "xyz"})
INTERNAL = frozenset({"cpanel", "autodiscover", "webmail", "mail", "smtp", "imap",
                      "pop", "ftp", "whm", "cpcontacts", "cpcalendars", "localhost"})
EXTRACT = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None,
                               include_psl_private_domains=True)
LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")


def apex_domain(value: str) -> str | None:
    # Wildcard SAN entries are ignored, not converted into extra discoveries.
    if not isinstance(value, str) or len(value) > 253 or "*" in value:
        return None
    try:
        name = value.strip().rstrip(".").encode("idna").decode("ascii").lower()
    except UnicodeError:
        return None
    labels = name.split(".")
    if len(name) > 253 or not all(LABEL.fullmatch(x) for x in labels):
        return None
    ext = EXTRACT(name)
    # Reject private hosting suffixes such as github.io rather than treating
    # every tenant as a registrable startup domain or collapsing to github.io.
    if ext.is_private or ext.suffix not in TLD_ALLOWLIST or not ext.domain:
        return None
    if INTERNAL.intersection(ext.subdomain.split(".")):
        return None
    return f"{ext.domain}.{ext.suffix}"


def certificate_domains(message: object) -> list[str]:
    if not isinstance(message, dict) or message.get("message_type") != "certificate_update":
        return []
    data = message.get("data")
    if not isinstance(data, dict):
        return []
    cert = data.get("leaf_cert")
    if not isinstance(cert, dict) or not isinstance(cert.get("all_domains"), list):
        return []
    return sorted({domain for value in cert["all_domains"]
                   if (domain := apex_domain(value)) is not None})
