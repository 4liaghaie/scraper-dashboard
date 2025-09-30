# scrapers/rebaid_details.py
from __future__ import annotations

import html as _html
import re
from typing import Dict, List, Tuple
from urllib.parse import urlparse, parse_qs, unquote, urljoin

from playwright.sync_api import sync_playwright

AMAZON_PAT = re.compile(r"(https?://(?:www\.)?(?:amazon\.[a-z.]+|amzn\.to)/[^\s\"']+)", re.I)

def _clean_one_line(s: str) -> str:
    import re as _re
    return _re.sub(r"\s+", " ", (s or "")).strip()

def _strip_tags(s: str) -> str:
    import re as _re
    s = _re.sub(r"<br\s*/?>", "\n", s, flags=_re.I)
    s = _re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s)
    return _re.sub(r"\s+", " ", s).strip()

def _extract_between(start_pat: str, end_pat: str, html_text: str) -> str:
    m = re.search(start_pat + r"(.*?)" + end_pat, html_text, re.S | re.I)
    return (m.group(1).strip() if m else "")

def _amazon_from_indirect(href: str) -> str | None:
    """Handle ?url=https://amazon... or ?u=... patterns."""
    try:
        if not href:
            return None
        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        cand = (qs.get("url") or qs.get("u") or [None])[0]
        if cand:
            cand = unquote(cand)
            if AMAZON_PAT.search(cand):
                return cand.strip()
    except Exception:
        pass
    return None

def _parse_product_html(html_text: str, base_url: str) -> Dict:
    # title
    title_html = (
        _extract_between(r'<div[^>]+class="[^"]*product-title[^"]*"[^>]*>.*?<h1[^>]*>', r"</h1>", html_text)
        or _extract_between(r'<div[^>]+class="[^"]*product-info[^"]*"[^>]*>.*?<h2[^>]*>', r"</h2>", html_text)
        or _extract_between(r"<h1[^>]*>", r"</h1>", html_text)
        or _extract_between(r"<h2[^>]*>", r"</h2>", html_text)
    )
    if not title_html:
        m = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.I)
        if m:
            title_html = m.group(1)
    title = _clean_one_line(_strip_tags(title_html))

    # description
    desc_html = (
        _extract_between(r'id="description"[^>]*>.*?<div[^>]+class="[^"]*content-wrapper[^"]*"[^>]*>', r"</div>", html_text)
        or _extract_between(r'<div[^>]+class="[^"]*product-description[^"]*"[^>]*>', r"</div>", html_text)
        or _extract_between(r'<div[^>]+class="[^"]*product-details[^"]*"[^>]*>', r"</div>", html_text)
    )
    if not desc_html:
        m = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.I)
        if m:
            desc_html = m.group(1)
    description = _clean_one_line(_strip_tags(desc_html))

    # amazon url
    amazon_url = ""
    for cls in (r"preview-link", r"buy-btn"):
        m = re.search(rf'<a[^>]+class="[^"]*{cls}[^"]*"[^>]+href="([^"]+)"', html_text, re.I)
        if m:
            href = m.group(1).strip()
            cand = _amazon_from_indirect(href) or href
            if AMAZON_PAT.search(cand or ""):
                amazon_url = cand
                break
    if not amazon_url:
        m = re.search(r'<a[^>]+href="([^"]*?(?:amazon\.[a-z.]+|amzn\.to)[^"]*)"', html_text, re.I)
        if m:
            amazon_url = m.group(1).strip()

    # image (first only)
    sec = re.search(
        r'(<section[^>]+class="[^"]*product-detail[^"]*"[^>]*>.*?</section>)',
        html_text, re.S | re.I
    ) or re.search(
        r'(<section[^>]+class="[^"]*product-detail-main[^"]*"[^>]*>.*?</section>)',
        html_text, re.S | re.I
    )
    blob = sec.group(1) if sec else html_text

    image_url = ""
    m = re.search(r'<img[^>]+(?:src|data-src|data-original|srcset|data-srcset)=["\']([^"\']+)["\']', blob, re.I)
    if m:
        raw = m.group(1).strip()
        if "," in raw or " " in raw:
            raw = raw.split(",")[0].split()[0]
        image_url = urljoin(base_url, raw)
    if not image_url:
        m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.I)
        if m:
            image_url = urljoin(base_url, m.group(1).strip())

    return {
        "title": title,
        "description": description,
        "amazon_url": amazon_url,
        "image_url": image_url,
    }

def scrape_rebaid_details(urls: List[str], *, timeout_ms: int = 12000) -> List[Dict]:
    """
    Returns list of {url, title, description, amazon_url, image_url}
    """
    out: List[Dict] = []
    if not urls:
        return out

    with sync_playwright() as p:
        ctx = p.request.new_context(
            extra_http_headers={
                "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/120.0.0.0 Safari/537.36"),
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://rebaid.com/",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )
        try:
            for u in urls:
                try:
                    r = ctx.get(u, timeout=timeout_ms)
                    if not r.ok:
                        continue
                    parsed = _parse_product_html(r.text(), base_url=u)
                    parsed["url"] = u
                    out.append(parsed)
                except Exception:
                    continue
        finally:
            ctx.dispose()
    return out
