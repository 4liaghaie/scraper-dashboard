# scrapers/rebatekey_details.py
from __future__ import annotations

import asyncio
import random
import re
from typing import List, Dict, Any
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
GEN_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
PRICE_RX = re.compile(r"\$\s*\d[\d,]*(?:\.\d{2})?")

def _soup(html: str) -> BeautifulSoup:
    try: return BeautifulSoup(html, "lxml")
    except Exception: return BeautifulSoup(html, "html.parser")

def _text(el) -> str | None:
    if not el: return None
    t = el.get_text(" ", strip=True)
    return t or None

def _extract_title(s: BeautifulSoup) -> str | None:
    return _text(s.select_one("h1.listing-title")) or _text(s.title)

def _extract_price(s: BeautifulSoup) -> str | None:
    price_el = s.select_one(".new-price, .price.text-green, .price, .listing-price, .rebate-price")
    if price_el:
        m = PRICE_RX.search(price_el.get_text(" ", strip=True))
        if m: return m.group(0).replace(" ", "")
    zone = s.select_one(".prod-description, .listing-title, .d-flex.align-items-center.mb-2-5, .row")
    if zone:
        m = PRICE_RX.search(zone.get_text(" ", strip=True))
        if m: return m.group(0).replace(" ", "")
    m = PRICE_RX.search(s.get_text(" ", strip=True))
    return m.group(0).replace(" ", "") if m else None

def _extract_first_image(s: BeautifulSoup, base_url: str) -> str | None:
    og = s.select_one('meta[property="og:image"][content]')
    if og and og.get("content"):
        return urljoin(base_url, og["content"])
    for sel in ["div.slider-main-img img", "div.product-gallery img", "figure img", "img"]:
        img = s.select_one(sel)
        if img and (img.get("src") or img.get("data-src")):
            return urljoin(base_url, img.get("src") or img.get("data-src"))
    return None

def _extract_amazon_url(s: BeautifulSoup) -> str | None:
    root = s.select_one('[id^="listing-"][data-url]')
    if root and root.get("data-url") and "amazon." in root["data-url"]:
        return root["data-url"]
    for a in s.select('a[href*="amazon."]'):
        href = a.get("href")
        if href and "amazon." in href:
            return href
    return None

def _extract_category(s: BeautifulSoup) -> str | None:
    for small in s.select("small"):
        if small.select_one(".fa-folder-tree"):
            link = small.select_one("a[href*='/coupons/'], a[href*='/rebates/']")
            nm = _text(link)
            if nm: return nm
    prod = s.select_one(".prod-description") or s
    link = prod.select_one("a[href*='/coupons/'], a[href*='/rebates/']")
    return _text(link)

def _extract_description(s: BeautifulSoup) -> str | None:
    selectors = [
        "div.col-xxl-6.col-xl-7.col-lg-8.col-md-10.col-sm-12.mx-auto.lato-medium",
        "div.mx-auto.lato-medium",
        ".listing-description",
        ".prod-description",
        '[itemprop="description"]',
    ]
    container = None
    for sel in selectors:
        el = s.select_one(sel)
        if el:
            container = el
            break

    def _from(el) -> str | None:
        parts: list[str] = []
        for child in el.children:
            name = getattr(child, "name", None)
            if name == "p":
                t = child.get_text(" ", strip=True)
                if t: parts.append(t)
            elif name in ("ul", "ol"):
                for li in child.find_all("li", recursive=False):
                    t = li.get_text(" ", strip=True)
                    if t: parts.append(f"- {t}")
        if parts: return "\n".join(parts)
        txt = el.get_text(" ", strip=True)
        if txt:
            for bad in ("What is the problem?", "Get Coupon Code", "Note: You have to register"):
                txt = txt.replace(bad, "")
            txt = re.sub(r"\s+", " ", txt).strip()
            return txt or None
        return None

    if container:
        got = _from(container)
        if got: return got

    candidates = []
    for el in s.select("div.mx-auto.lato-medium, div[class*='lato-medium']"):
        txt = el.get_text(" ", strip=True)
        if not txt: continue
        if "What is the problem?" in txt or "Note: You have to register" in txt: continue
        candidates.append((len(txt), txt))
    if candidates:
        candidates.sort(reverse=True, key=lambda t: t[0])
        return re.sub(r"\s+", " ", candidates[0][1]).strip()
    return None

async def _fetch_text(client: httpx.AsyncClient, url: str, retries: int, timeout: float) -> str | None:
    for attempt in range(retries + 1):
        try:
            r = await client.get(url, timeout=timeout, follow_redirects=True)
            if r.status_code == 200:
                txt = r.text or ""
                if txt.strip(): return txt
        except Exception:
            pass
        await asyncio.sleep(0.3 + attempt * 0.5 + random.random() * 0.2)
    return None

async def _parse_one(client: httpx.AsyncClient, url: str, sem: asyncio.Semaphore, retries: int, timeout: float) -> Dict[str, Any]:
    async with sem:
        html = await _fetch_text(client, url, retries=retries, timeout=timeout)
    if not html:
        return {"url": url, "error": "fetch_failed"}

    s = _soup(html)
    return {
        "url": url,
        "title": _extract_title(s),
        "price": _extract_price(s),
        "image_url": _extract_first_image(s, url),
        "amazon_url": _extract_amazon_url(s),
        "category": _extract_category(s),
        "description": _extract_description(s),
    }

async def _amain(urls: List[str], concurrency: int, retries: int, timeout: float) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(max(1, concurrency))
    async with httpx.AsyncClient(headers=GEN_HEADERS, http2=True) as client:
        tasks = [asyncio.create_task(_parse_one(client, u, sem, retries, timeout)) for u in urls]
        out: List[Dict[str, Any]] = []
        for fut in asyncio.as_completed(tasks):
            out.append(await fut)
        # keep input order
        order = {u: i for i, u in enumerate(urls)}
        out.sort(key=lambda r: order.get(r.get("url",""), 1_000_000))
        return out

def collect_rebatekey_details(urls: List[str], *, concurrency: int = 12, retries: int = 2, timeout: float = 20.0) -> List[Dict[str, Any]]:
    """Sync wrapper returning list of {url,title,price,image_url,amazon_url,category,description}"""
    if not urls: return []
    return asyncio.run(_amain(urls, concurrency, retries, timeout))
