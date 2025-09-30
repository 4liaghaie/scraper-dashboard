# scrapers/myvipon_details.py
from __future__ import annotations

import re, time, json
from typing import Any, Dict, List, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

# Optional HTTP stacks (best → fallback)
try:
    from curl_cffi import requests as curlreq
    _HAS_CURLCFFI = True
except Exception:
    _HAS_CURLCFFI = False

try:
    import cloudscraper
    _HAS_CLOUDSCRAPER = True
except Exception:
    _HAS_CLOUDSCRAPER = False

import requests

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
BASE_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
DEFAULT_REFERER = "https://www.myvipon.com/"

# ---------- helpers & parsing ----------

def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _first_attr(el, names):
    for n in names:
        if el and el.has_attr(n) and el.get(n):
            return el.get(n)
    return None

def _parse_price(soup: BeautifulSoup) -> str:
    blk = soup.select_one("p.product-price")
    if not blk:
        m = re.search(r"\$[\d,.]+", soup.get_text(" ", strip=True))
        return m.group(0) if m else ""
    for sp in blk.select("span"):
        t = _clean_text(sp.get_text())
        if re.search(r"^\$?\d[\d,]*([.]\d{2})?$", t):
            return t if t.startswith("$") else f"${t}"
    t = _clean_text(blk.get_text(" "))
    m = re.search(r"\$[\d,.]+", t)
    return m.group(0) if m else t

def _price_value(price_str: str) -> float | None:
    if not price_str:
        return None
    s = price_str.replace("$", "").replace(",", "").strip()
    m = re.search(r"\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

def _parse_description(soup: BeautifulSoup) -> str:
    bullets = []
    for li in soup.select("#bulletPoint li"):
        txt = _clean_text(li.get_text(" ", strip=True))
        if txt:
            bullets.append(txt)
    if bullets:
        return " • ".join(bullets)
    desc_div = soup.select_one(".shop_name .desc-div")
    if desc_div:
        parts = [_clean_text(x.get_text(" ", strip=True)) for x in desc_div.select("li, p")]
        parts = [p for p in parts if p and p.lower() not in {"about the product", "description"}]
        if parts:
            return " ".join(parts)
    return ""

def _parse_category(soup: BeautifulSoup) -> str:
    trail = soup.select(".Breadcrumb a")
    if trail:
        last = _clean_text(trail[-1].get_text())
        if last and last.lower() not in {"vipon", "categories"}:
            return last
    bc = soup.select_one(".Breadcrumb")
    if bc:
        txt = _clean_text(bc.get_text(" "))
        parts = [p for p in (x.strip() for x in re.split(r">\s*", txt)) if p]
        if parts:
            return parts[-1]
    return ""

def _looks_like_amazon(u: str) -> bool:
    try:
        netloc = urlparse(u).netloc.lower()
        return "amazon." in netloc or netloc.endswith("amzn.to")
    except Exception:
        return False

def _parse_amazon_url(soup: BeautifulSoup) -> str:
    btn = soup.select_one("#plummet-status")
    if btn and btn.has_attr("onclick"):
        oc = btn.get("onclick", "")
        m = re.search(r"detailClickRecord\([^,]*,[^,]*,'([^']+)'", oc)
        if m and _looks_like_amazon(m.group(1)):
            return m.group(1)
    a = soup.select_one("p.go-to-amazon a, .go-to-amazon a")
    if a and a.has_attr("href") and _looks_like_amazon(a["href"]):
        return a["href"]
    for a in soup.select("a[href]"):
        href = a["href"].strip()
        if _looks_like_amazon(href):
            return href
    return ""

def _fetch_html(url: str, referer: str, timeout: int = 30, proxy: str | None = None) -> str:
    proxies = {"http": proxy, "https": proxy} if proxy else None

    if _HAS_CURLCFFI:
        try:
            with curlreq.Session(impersonate="chrome124", http2=True, proxies=proxies) as s:
                h = dict(BASE_HEADERS); h["Referer"] = referer
                r = s.get(url, headers=h, timeout=timeout)
                if r.status_code == 200 and r.text:
                    return r.text
                if r.status_code in (403, 503):
                    raise requests.HTTPError(f"{r.status_code} from curl_cffi")
        except Exception:
            pass

    if _HAS_CLOUDSCRAPER:
        try:
            s = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})
            h = dict(BASE_HEADERS); h["Referer"] = referer
            r = s.get(url, headers=h, timeout=timeout, proxies=proxies)
            r.raise_for_status()
            return r.text
        except Exception:
            pass

    s = requests.Session()
    h = dict(BASE_HEADERS); h["Referer"] = referer
    r = s.get(url, headers=h, timeout=timeout, proxies=proxies)
    r.raise_for_status()
    return r.text

def _parse_page(url: str, html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")

    img_el = soup.select_one(".left-show-img img")
    img_url = _first_attr(img_el, ["src", "data-src", "data-original"]) or ""
    if img_url:
        img_url = urljoin(url, img_url)

    title_el = soup.select_one(".product-title span") or soup.select_one(".product-title")
    title = _clean_text(title_el.get_text()) if title_el else ""

    price = _parse_price(soup)
    description = _parse_description(soup)
    category = _parse_category(soup)
    amazon_url = _parse_amazon_url(soup)

    return {
        "image_url": img_url,
        "title": title,
        "price": price,
        "price_value": _price_value(price),
        "description": description,
        "category": category,
        "amazon_url": amazon_url,
    }

def _scrape_one(
    url: str,
    referer: str,
    timeout: int,
    proxy: str | None,
    max_retries: int,
    backoff: float,
) -> Dict[str, Any]:
    attempt = 0
    last_err = None
    while attempt <= max_retries:
        try:
            html = _fetch_html(url, referer=referer, timeout=timeout, proxy=proxy)
            data = _parse_page(url, html)
            data.update({"url": url, "status": "ok"})
            return data
        except Exception as e:
            last_err = e
            if attempt == max_retries:
                break
            time.sleep(backoff * (2 ** attempt))
            attempt += 1
    return {"url": url, "status": "error", "error": str(last_err)}

def scrape_details_for_urls(
    urls: Iterable[str],
    *,
    workers: int = 5,
    referer: str = DEFAULT_REFERER,
    timeout: int = 30,
    proxy: str | None = None,
    retries: int = 2,
    backoff: float = 1.0,
) -> List[Dict[str, Any]]:
    urls = [u for u in urls if isinstance(u, str) and u.startswith("http")]
    if not urls:
        return []
    out: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(_scrape_one, u, referer, timeout, proxy, retries, backoff): u
            for u in urls
        }
        for fut in as_completed(futs):
            out.append(fut.result())
    return out
