# scrapers/rebaid_urls.py
from __future__ import annotations

import json
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from settings import settings

# --- constants / detection ---

# Skip "Featured Deals" blocks when scanning upwards from a match
_FEATURED_RE = re.compile(r"Featured\s*Deals", re.I)

# Card anchors on Rebaid list pages (intentionally matches misspelt 'treding')
_CARD_RE = re.compile(
    r'(<a\b[^>]+class="[^"]*\btreding-product-box\b[^"]*"[^>]*>)(.*?)</a>',
    re.S | re.I,
)

# Pagination container
_PAGINATION_RE = re.compile(
    r'<ul[^>]+class="[^"]*pagination-list[^"]*"[^>]*>(.*?)</ul>', re.S | re.I
)

# Internal bucket detection from href path
_PATH_BITS = (
    ("codes", "/discount_detail/"),
    ("cashback", "/product_detail/"),
    ("buyonrebaid", "/rebaid-product-detail/"),
)

# $12.34 (allows thousands separators)
_PRICE_RE = re.compile(r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)")

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

# --- helpers ---


def _to_base(url: str) -> str:
    u = urlparse(url)
    return f"{u.scheme}://{u.netloc}"


def _set_query(url: str, **params) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in params.items():
        q[str(k)] = [str(v)]
    new_query = urlencode({k: v[0] for k, v in q.items()})
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


def _abs_url(base: str, href: str) -> str:
    return urljoin(base, (href or "").strip())


def _http_get(session: requests.Session, url: str, timeout_s: float) -> str:
    r = session.get(url, timeout=timeout_s)
    r.raise_for_status()
    r.encoding = r.encoding or "utf-8"
    return r.text


def _clean_text(s: str) -> str:
    s = re.sub(r"<br\s*/?>", " ", s or "", flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_price_from_known_containers(inner_html: str) -> str:
    """
    Try legacy DOM locations first (more precise when present).
    """
    m = re.search(
        r'<div[^>]+class="[^"]*product-footer[^"]*"[^>]*>.*?<strong[^>]*>(.*?)</strong>',
        inner_html,
        re.S | re.I,
    )
    if m:
        return _clean_text(m.group(1))

    m = re.search(
        r'<[^>]+class="[^"]*\bfull-price\b[^"]*"[^>]*>(.*?)</',
        inner_html,
        re.S | re.I,
    )
    if m:
        return _clean_text(m.group(1))

    m = re.search(r"<strong[^>]*>(.*?)</strong>", inner_html, re.S | re.I)
    if m:
        return _clean_text(m.group(1))

    return ""


def _extract_price_text_and_value(inner_html: str) -> tuple[str, float | None]:
    """
    Robust price extraction:
      1) Try known containers (<strong>, .full-price, product-footer).
      2) Fallback: parse visible text for $… amounts; choose the *last* amount
         (first is often list price; last is deal/rebate price).
      3) Treat FREE specially.
    Returns: (price_text, price_value)
    """
    # 1) Known containers
    txt = _extract_price_from_known_containers(inner_html)
    if not txt:
        # 2) Fallback to visible text
        txt = _clean_text(inner_html)

    low = txt.lower()
    if "free" in low:
        return ("FREE", 0.0)

    # Find all $ amounts and take the last as effective price
    amounts = _PRICE_RE.findall(txt)
    if amounts:
        last_txt = amounts[-1]
        try:
            last_val = float(last_txt.replace(",", ""))
        except Exception:
            last_val = None
        return (f"${last_txt}", last_val)

    # No dollar amounts; return cleaned text (maybe something like "100% Cash Back")
    return (txt.strip(), None if "$" not in txt else None)


def _looks_featured_context(html: str, anchor_start: int) -> bool:
    start = max(0, anchor_start - 1200)
    return bool(_FEATURED_RE.search(html[start:anchor_start]))


def _detect_bucket_from_href(u: str) -> str:
    for cat, bit in _PATH_BITS:
        if bit in (u or ""):
            return cat
    return ""  # unknown / ignore


def _parse_listing_page(html: str, base: str) -> List[Dict[str, Any]]:
    """
    Parse one listing page and return:
      [{ "url", "category" (codes/cashback/buyonrebaid), "price", "price_value" }, ...]
    """
    out: List[Dict[str, Any]] = []
    for m in _CARD_RE.finditer(html):
        a_open, inner = m.group(1), (m.group(2) or "")

        if _looks_featured_context(html, m.start()):
            continue

        href_m = re.search(r'href\s*=\s*["\']([^"\']+)["\']', a_open, re.I)
        if not href_m:
            continue

        href = _abs_url(base, href_m.group(1))
        bucket = _detect_bucket_from_href(href)
        if not bucket:
            continue

        price_text, price_value = _extract_price_text_and_value(inner)

        out.append(
            {
                "url": href,
                "category": bucket,      # internal bucket (your original 'category')
                "price": price_text,     # string like "$11.99" or "FREE" or ""
                "price_value": price_value,  # numeric if parsed, else None
            }
        )
    return out


def _parse_pagination(html: str, base: str, first_page_url: str) -> Tuple[int, int, Dict[int, str]]:
    """
    Return: (current_page, last_page, {page_number: url})
    Fallback: {1: first_page_url}
    """
    ul_m = _PAGINATION_RE.search(html)
    if not ul_m:
        return 1, 1, {1: first_page_url}

    ul = ul_m.group(1)
    page_map: Dict[int, str] = {}
    current = 1
    last = 1

    a_active = re.search(r'<a[^>]*class="[^"]*\bactive\b[^"]*"[^>]*>(\d+)</a>', ul, re.I)
    if a_active:
        try:
            current = int(a_active.group(1))
        except Exception:
            current = 1

    for a in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(\d+)</a>', ul, re.S | re.I):
        href = _abs_url(base, a.group(1))
        try:
            num = int(a.group(2))
        except Exception:
            continue
        page_map[num] = href
        last = max(last, num)

    page_map.setdefault(1, first_page_url)
    return current or 1, last or 1, page_map


def _dedup_keep_first(items: List[Dict[str, Any]], key: str = "url") -> List[Dict[str, Any]]:
    seen, out = set(), []
    for it in items:
        u = (it.get(key) or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(it)
    return out


# --- public entrypoint for backend ---


def collect_rebaid_urls(
    categories: List[Dict[str, str]],
    *,
    max_pages: int = 0,
    timeout_ms: int = 30000,
    delay_min: float = 0.15,
    delay_max: float = 0.45,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Args:
      categories: [{"name": "...", "url": "https://rebaid.com/?category=..."}]
    Returns buckets:
      {
        "codes":       [{"url","price","price_value","category","category_name"}],
        "cashback":    [{"url","price","price_value","category","category_name"}],
        "buyonrebaid": [{"url","price","price_value","category","category_name"}],
      }
    Where:
      - category       : "codes" | "cashback" | "buyonrebaid" (internal bucket)
      - category_name  : input category name (e.g., "Home & Kitchen")
      - price          : "$11.99" | "FREE" | "" (string)
      - price_value    : float | None
    """
    keep_buckets = ["codes", "cashback", "buyonrebaid"]
    out: Dict[str, List[Dict[str, Any]]] = {b: [] for b in keep_buckets}
    timeout_s = max(1.0, timeout_ms / 1000.0)

    with requests.Session() as s:
        s.headers.update(_HEADERS)

        for cat in categories:
            cat_name = str(cat.get("name", "")).strip()
            cat_url = str(cat.get("url", "")).strip()
            if not cat_name or not cat_url:
                continue

            base = _to_base(cat_url)

            try:
                html1 = _http_get(s, cat_url, timeout_s)
            except Exception:
                continue

            _, last, page_map = _parse_pagination(html1, base, cat_url)
            target_last = last if max_pages <= 0 else min(last, max_pages)

            for page_num in range(1, target_last + 1):
                url = page_map.get(page_num) or _set_query(cat_url, page=page_num)
                try:
                    html = html1 if page_num == 1 else _http_get(s, url, timeout_s)
                except Exception:
                    continue

                for card in _parse_listing_page(html, base):
                    bucket = card.get("category") or ""
                    if bucket in keep_buckets:
                        out[bucket].append(
                            {
                                "url": card["url"],
                                "price": card.get("price", ""),
                                "price_value": card.get("price_value"),
                                "category": bucket,         # keep for compatibility
                                "category_name": cat_name,  # input category
                            }
                        )

                time.sleep(random.uniform(max(0.0, delay_min), max(delay_min, delay_max)))

    # final dedup per bucket by URL
    for b in list(out.keys()):
        out[b] = _dedup_keep_first(out[b], key="url")
    return out


def load_default_rebaid_categories() -> list[dict[str, str]]:
    """
    Loads categories from:
      1) settings.rebaid_categories_path if set
      2) scrapers/data/rebaid_categories.json (alongside this module)
    Returns [{"name": "...", "url": "..."}].
    """
    # user override via .env
    if settings.rebaid_categories_path:
        p = Path(settings.rebaid_categories_path)
    else:
        p = Path(__file__).parent / "data" / "rebaid_categories.json"

    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)

    cats = data.get("categories", data)
    out: list[dict[str, str]] = []
    if isinstance(cats, list):
        for row in cats:
            if isinstance(row, dict):
                name = str(row.get("name", "")).strip()
                url = str(row.get("url", "")).strip()
                if name and url:
                    out.append({"name": name, "url": url})
    elif isinstance(cats, dict):
        for name, url in cats.items():
            name_s, url_s = str(name).strip(), str(url).strip()
            if name_s and url_s:
                out.append({"name": name_s, "url": url_s})
    else:
        raise ValueError("Invalid categories JSON shape")
    if not out:
        raise ValueError("No valid categories found in rebaid_categories.json")
    return out
