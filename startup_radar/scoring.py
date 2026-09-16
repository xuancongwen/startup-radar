"""Explainable, unique-phrase scoring; no network calls or LLM costs."""
import re
from dataclasses import dataclass

from bs4 import BeautifulSoup

RULES = {
    "founder_hiring": (2, ["we are hiring", "join our team", "careers", "about us", "founder"]),
    "early_stage": (4, ["join waitlist", "join the waitlist", "private beta", "early access",
                        "coming soon", "backed by", "launching in"]),
    "tech_product": (3, ["AI platform", "API", "developer tool", "automation", "workflow",
                         "infrastructure", "SaaS"]),
}
PARKED = ["domain for sale", "buy this domain", "parked free", "under construction",
          "hugedomains", "sedo", "godaddy", "namecheap parking"]


def contains(text: str, phrase: str) -> bool:
    return re.search(r"(?<!\w)" + re.escape(phrase.lower()) + r"(?!\w)", text) is not None


@dataclass
class Analysis:
    title: str
    description: str
    og_tags: dict[str, str]
    visible_text: str
    score: int
    matched_signals: list[dict]
    status: str


def analyze(html: bytes, threshold: int) -> Analysis:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True)[:1000] if soup.title else ""
    description = ""
    og = {}
    for meta in soup.find_all("meta"):
        name = str(meta.get("name", "")).lower()
        prop = str(meta.get("property", "")).lower()
        content = str(meta.get("content", ""))[:2000]
        if name == "description":
            description = content
        if prop.startswith("og:"):
            og[prop] = content
    for node in list(soup.find_all(["script", "style", "noscript", "template", "svg", "head"])):
        node.decompose()
    for node in list(soup.find_all(True)):
        if node.attrs is None:
            continue  # parent already removed
        style = str(node.get("style", "")).replace(" ", "").lower()
        if (node.has_attr("hidden") or str(node.get("aria-hidden", "")).lower() == "true"
                or "display:none" in style or "visibility:hidden" in style):
            node.decompose()
    visible = " ".join(soup.stripped_strings)
    text = " ".join((title + " " + description + " " + " ".join(og.values()) + " " + visible)
                    .lower().split())
    parked = [{"category": "parked", "phrase": p, "points": 0}
              for p in PARKED if contains(text, p)]
    if parked:
        return Analysis(title, description, og, visible, 0, parked, "parked")
    signals = [{"category": category, "phrase": phrase, "points": points}
               for category, (points, phrases) in RULES.items()
               for phrase in phrases if contains(text, phrase)]
    score = sum(signal["points"] for signal in signals)
    return Analysis(title, description, og, visible, score, signals,
                    "startup_candidate" if score > threshold else "live")
