# scrapers/amazon_store.py
from __future__ import annotations

import re
import asyncio
from typing import Dict, List, Tuple, Optional, Union

from urllib.parse import urlparse, urljoin

# Playwright (async)
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout


# ----------------------------- helpers -----------------------------

def _amazon_base(amazon_url: str) -> str:
    u = urlparse(amazon_url)
    return f"{u.scheme}://{u.netloc}"


def _strip_tags(s: str) -> str:
    import re as _re, html as _html
    s = _re.sub(r"<br\s*/?>", " ", s, flags=_re.I)
    s = _re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s)
    return _re.sub(r"\s+", " ", s).strip()


def _clean_one_line(s: str) -> str:
    import re as _re
    return _re.sub(r"\s+", " ", (s or "")).strip()


def parse_amazon_store(html_text: str, amazon_url: str) -> Tuple[str, str]:
    """
    Returns (store_name, store_url) or ("","") if not found.
    Handles both brand store and seller profile links.
    """
    # 1) Brand "byline" link (common for brand stores)
    m = re.search(
        r'<a[^>]+id=["\']bylineInfo["\'][^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        html_text, re.S | re.I
    )
    if m:
        href = m.group(1).strip()
        text = _strip_tags(m.group(2))
        import re as _re
        t = _re.sub(r'^\s*Brand:\s*', '', text, flags=_re.I)
        t = _re.sub(r'^\s*Visit\s+the\s+', '', t, flags=_re.I)
        t = _re.sub(r'\s+Store\s*$', '', t, flags=_re.I)
        store_name = _clean_one_line(t)
        store_url = urljoin(_amazon_base(amazon_url), href)
        return store_name, store_url

    # 2) Seller profile link (fallback)
    m = re.search(
        r'<a[^>]+id=["\']sellerProfileTriggerId["\'][^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        html_text, re.S | re.I
    )
    if m:
        href = m.group(1).strip()
        text = _strip_tags(m.group(2))
        store_name = _clean_one_line(text)
        store_url = urljoin(_amazon_base(amazon_url), href)
        return store_name, store_url

    return "", ""


# Common anti-bot signals seen on Amazon
_ANTIBOT_PATTERNS = [
    r"Robot\s*Check",
    r"Amazon\s*Captcha",
    r"automatically\s+recognized\s+as\s+a\s+robot",
    r"make\s+sure\s+you\s*'?re\s+not\s+a\s+robot",
    r"/errors/validateCaptcha",
    r"Enter\s+the\s+characters\s+you\s+see\s+below",
    r"Sorry!\s+Something\s+went\s+wrong",
]

def _looks_like_antibot(status: int, html: str) -> bool:
    if status in (403, 429, 503):
        return True
    text = html or ""
    for pat in _ANTIBOT_PATTERNS:
        if re.search(pat, text, re.I):
            return True
    return False


# ------------------------- core async worker -------------------------

async def _scrape_many_async(
    amazon_urls: List[str],
    *,
    timeout_ms: int = 12000,
    concurrency: int = 16,
    retries: int = 5,
    escalate_to_page: bool = True,
    headless: bool = True,
    proxy: Optional[str] = None,
    return_diagnostics: bool = False,
) -> Union[
    Dict[str, Dict[str, str]],
    Tuple[Dict[str, Dict[str, str]], Dict[str, object]]
]:
    """
    Internal async implementation with bounded concurrency, simple retries,
    anti-bot detection, and optional page-render escalation.

    Return:
      - if return_diagnostics=False (default): Dict[url] -> {amazon_store_name, amazon_store_url, ...diagnostics}
      - if return_diagnostics=True: (per_url_map, aggregate_stats)
          where aggregate_stats includes counts AND URL lists like 'antibot_urls'.
    """
    out: Dict[str, Dict[str, str]] = {}
    uniq = [u for u in dict.fromkeys(amazon_urls or []) if u]
    if not uniq:
        empty_stats = {
            "total": 0, "found": 0,
            "antibot_hits": 0, "timeouts": 0, "http_errors": 0, "no_store_found": 0,
            "antibot_urls": [], "timeout_urls": [], "http_error_urls": [], "no_store_urls": []
        }
        return (out, empty_stats) if return_diagnostics else out

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.amazon.com/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    async with async_playwright() as p:
        # HTTP request context (cheaper than full browser)
        req_kwargs = {"extra_http_headers": headers}
        if proxy:
            req_kwargs["proxy"] = {"server": proxy}
        req_ctx = await p.request.new_context(**req_kwargs)

        # Lazy browser context for escalations
        browser = None
        page_ctx = None
        page_lock = asyncio.Lock()

        async def ensure_page_context():
            nonlocal browser, page_ctx
            async with page_lock:
                if browser is None:
                    browser = await p.chromium.launch(headless=headless)
                    ctx_kwargs = {}
                    if proxy:
                        ctx_kwargs["proxy"] = {"server": proxy}
                    page_ctx = await browser.new_context(**ctx_kwargs)
            return page_ctx

        sem = asyncio.Semaphore(max(1, int(concurrency)))

        async def fetch_one(u: str):
            stats = {
                "antibot_hits": 0,
                "timeouts": 0,
                "http_errors": 0,
                "no_store_found": 0,
            }
            store_name = ""
            store_url = ""

            # Try lightweight HTTP first, with retries
            attempt = 0
            while attempt < max(1, int(retries)):
                attempt += 1
                try:
                    r = await req_ctx.get(u, timeout=timeout_ms)
                    status = r.status
                    if not r.ok:
                        stats["http_errors"] += 1
                        if status == 404:
                            break
                        continue

                    html = await r.text()
                    final_url = r.url or u

                    if _looks_like_antibot(status, html):
                        stats["antibot_hits"] += 1
                        continue

                    store_name, store_url = parse_amazon_store(html, final_url)
                    break

                except PlaywrightTimeout:
                    stats["timeouts"] += 1
                except Exception:
                    stats["http_errors"] += 1

            # If still nothing and allowed, try a rendered page once
            if escalate_to_page and not store_name and not store_url:
                try:
                    ctx = await ensure_page_context()
                    page = await ctx.new_page()
                    await page.goto(u, timeout=timeout_ms, wait_until="domcontentloaded")
                    html2 = await page.content()
                    final2 = page.url or u
                    await page.close()

                    if _looks_like_antibot(200, html2):
                        stats["antibot_hits"] += 1
                    else:
                        store_name, store_url = parse_amazon_store(html2, final2)
                except PlaywrightTimeout:
                    stats["timeouts"] += 1
                except Exception:
                    stats["http_errors"] += 1

            if not store_name and not store_url:
                stats["no_store_found"] = 1

            out[u] = {
                "amazon_store_name": store_name,
                "amazon_store_url": store_url,
                **stats,
            }

        async def bounded(u: str):
            async with sem:
                await fetch_one(u)

        await asyncio.gather(*(bounded(u) for u in uniq))

        # Cleanup
        await req_ctx.dispose()
        if page_ctx is not None:
            await page_ctx.close()
        if browser is not None:
            await browser.close()

    if return_diagnostics:
        antibot_urls = [u for u, v in out.items() if int(v.get("antibot_hits", 0)) > 0]
        timeout_urls = [u for u, v in out.items() if int(v.get("timeouts", 0)) > 0]
        http_error_urls = [u for u, v in out.items() if int(v.get("http_errors", 0)) > 0]
        no_store_urls = [u for u, v in out.items() if int(v.get("no_store_found", 0)) > 0]

        agg = {
            "total": len(out),
            "found": sum(1 for v in out.values() if (v.get("amazon_store_name") or v.get("amazon_store_url"))),
            "antibot_hits": sum(int(v.get("antibot_hits", 0)) for v in out.values()),
            "timeouts": sum(int(v.get("timeouts", 0)) for v in out.values()),
            "http_errors": sum(int(v.get("http_errors", 0)) for v in out.values()),
            "no_store_found": sum(int(v.get("no_store_found", 0)) for v in out.values()),
            # URL lists (so your endpoint can slice samples)
            "antibot_urls": antibot_urls,
            "timeout_urls": timeout_urls,
            "http_error_urls": http_error_urls,
            "no_store_urls": no_store_urls,
        }
        return out, agg
    return out


# ----------------------------- public APIs -----------------------------

async def scrape_amazon_store_many_async(
    amazon_urls: List[str],
    *,
    timeout_ms: int = 12000,
    concurrency: int = 16,
    retries: int = 5,
    escalate_to_page: bool = True,
    headless: bool = True,
    proxy: Optional[str] = None,
    return_diagnostics: bool = False,
):
    """
    Public async API.

    Example:
        store_map, stats = await scrape_amazon_store_many_async(
            urls, return_diagnostics=True
        )
    """
    return await _scrape_many_async(
        amazon_urls,
        timeout_ms=timeout_ms,
        concurrency=concurrency,
        retries=retries,
        escalate_to_page=escalate_to_page,
        headless=headless,
        proxy=proxy,
        return_diagnostics=return_diagnostics,
    )


def scrape_amazon_store_many(
    amazon_urls: List[str],
    *,
    timeout_ms: int = 12000,
    concurrency: int = 16,
    retries: int = 5,
    escalate_to_page: bool = True,
    headless: bool = True,
    proxy: Optional[str] = None,
    return_diagnostics: bool = False,
):
    """
    Public sync API (compatible with existing code).

    If return_diagnostics=True, returns (store_map, stats).
    Otherwise returns just store_map.

    Safe to call from worker threads via asyncio.to_thread(...).
    """
    try:
        return asyncio.run(
            _scrape_many_async(
                amazon_urls,
                timeout_ms=timeout_ms,
                concurrency=concurrency,
                retries=retries,
                escalate_to_page=escalate_to_page,
                headless=headless,
                proxy=proxy,
                return_diagnostics=return_diagnostics,
            )
        )
    except RuntimeError:
        # Fallback if a loop is already running in this thread
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(
                _scrape_many_async(
                    amazon_urls,
                    timeout_ms=timeout_ms,
                    concurrency=concurrency,
                    retries=retries,
                    escalate_to_page=escalate_to_page,
                    headless=headless,
                    proxy=proxy,
                    return_diagnostics=return_diagnostics,
                )
            )
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                asyncio.set_event_loop(None)
                loop.close()
