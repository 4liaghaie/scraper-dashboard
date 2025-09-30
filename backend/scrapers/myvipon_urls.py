# scrapers/myvipon_urls.py
from __future__ import annotations

import json, random, re, time
from pathlib import Path
from typing import Dict, List

import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from settings import settings

# ---------------------- utilities ----------------------

def base_of(url: str) -> str:
    from urllib.parse import urlsplit
    p = urlsplit(url)
    return f"{p.scheme}://{p.netloc}"

def safe_name(name: str) -> str:
    n = name.replace("&", "and")
    n = re.sub(r"[^A-Za-z0-9]+", "-", n)
    n = re.sub(r"-{2,}", "-", n).strip("-")
    return n or "category"

# ---------------------- page probes & scroll helpers ----------------------

def _install_scroll_helpers(driver):
    driver.execute_script("""
      (function(){
        if (window.__scroll) return;
        function findScrollRoot(){
          if (window.__SCROLL_ROOT__ && document.contains(window.__SCROLL_ROOT__)) return window.__SCROLL_ROOT__;
          const cands = [document.scrollingElement, document.documentElement, document.body];
          for (const el of cands){
            if (!el) continue;
            const overflow = el.scrollHeight - el.clientHeight;
            if (overflow > 100) { window.__SCROLL_ROOT__ = el; return el; }
          }
          let best=null,bestScore=0;
          for (const el of document.querySelectorAll('main,section,div')){
            try{
              const st=getComputedStyle(el);
              if(!/(auto|scroll)/.test(st.overflowY)) continue;
              const overflow=el.scrollHeight-el.clientHeight;
              if(overflow<100) continue;
              const r=el.getBoundingClientRect();
              const score=(r.width*r.height)+overflow;
              if(score>bestScore){best=el;bestScore=score;}
            }catch(e){}
          }
          window.__SCROLL_ROOT__ = best || document.scrollingElement || document.documentElement || document.body;
          return window.__SCROLL_ROOT__;
        }
        window.__scroll = {
          root: function(){ return findScrollRoot(); },
          gap: function(){ const el=findScrollRoot(); return Math.floor(el.scrollHeight-(el.scrollTop+el.clientHeight)); },
          by: function(delta){ const el=findScrollRoot(); el.scrollTop=Math.min(el.scrollTop+delta, el.scrollHeight); return el.scrollTop; },
          toEnd: function(){ const el=findScrollRoot(); el.scrollTop=el.scrollHeight; }
        };
      })();
    """)

def is_end_banner_visible(driver) -> bool:
    try:
        return bool(driver.execute_script("""
            const el = document.querySelector('#loading-notify');
            if (!el) return false;
            const st = getComputedStyle(el);
            return st && st.display && st.display.toLowerCase() === 'flex';
        """))
    except Exception:
        return False

def get_card_count(driver) -> int:
    try:
        return int(driver.execute_script(
            "return document.querySelectorAll('div.box.solid, div[id^=\"product-\"]').length;"
        ))
    except Exception:
        return 0

def bottom_gap(driver) -> int:
    try:
        return int(driver.execute_script("return window.__scroll ? window.__scroll.gap() : 99999;"))
    except Exception:
        return 99999

def wheel_scroll_from_element(driver, element, delta_y: int):
    try:
        driver.execute_script("return window.__scroll && window.__scroll.by(arguments[0]);", int(delta_y))
    except Exception:
        driver.execute_script("window.scrollBy(0, arguments[0]);", int(delta_y))

# ---------------------- minimized-window keep-alive helpers ----------------------

def _spoof_visibility_and_focus(driver):
    try:
        driver.execute_script("""
          (function(){
            if (window.__VIS_PATCHED__) return;
            try { Object.defineProperty(document, 'hidden', { get: () => false }); } catch(e){}
            try { Object.defineProperty(document, 'visibilityState', { get: () => 'visible' }); } catch(e){}
            try { document.hasFocus = () => true; } catch(e){}
            try {
              addEventListener('visibilitychange', e => { e.stopImmediatePropagation(); }, true);
              addEventListener('pagehide', e => { e.stopImmediatePropagation(); }, true);
              addEventListener('freeze', e => { e.stopImmediatePropagation(); }, true);
            } catch(e){}
            window.__VIS_PATCHED__ = true;
          })();
        """)
    except Exception:
        pass

def _ensure_awake_and_viewport(driver, width=1366, height=1000):
    # Bring tab to front / keep lifecycle active
    try:
        driver.execute_cdp_cmd("Page.bringToFront", {})
    except Exception:
        pass
    try:
        driver.execute_cdp_cmd("Page.setWebLifecycleState", {"state": "active"})
    except Exception:
        pass

    # Ensure sane viewport even if minimized/occluded
    try:
        vw, vh = driver.execute_script("return [window.innerWidth, window.innerHeight];")
    except Exception:
        vw, vh = (0, 0)

    if not vw or not vh or vw < 500 or vh < 500:
        try:
            driver.execute_cdp_cmd(
                "Emulation.setDeviceMetricsOverride",
                {"width": int(width), "height": int(height), "deviceScaleFactor": 1, "mobile": False},
            )
        except Exception:
            pass

    _spoof_visibility_and_focus(driver)

def _cdp_wheel(driver, delta_y=800):
    try:
        vw, vh = driver.execute_script("return [window.innerWidth, window.innerHeight];")
        x = int((vw or 1200) / 2)
        y = int((vh or 800) - 120)
        driver.execute_cdp_cmd("Input.dispatchMouseEvent", {"type": "mouseMoved","x": x,"y": y,"buttons": 0})
        driver.execute_cdp_cmd("Input.dispatchMouseWheelEvent", {"x": x,"y": y,"deltaX": 0,"deltaY": int(delta_y),"pointerType": "mouse"})
    except Exception:
        pass

# ---------------------- scrolling logic ----------------------

def blast_to_bottom_once(driver, max_burst_steps: int, min_delta: int, max_delta: int,
                         micro_pause_min: float, micro_pause_max: float):
    steps = 0
    while True:
        gap = bottom_gap(driver)
        if gap <= 10:
            break
        delta = min(max_delta, max(min_delta, int(gap * random.uniform(0.4, 0.9))))
        wheel_scroll_from_element(driver, None, delta)
        _cdp_wheel(driver, delta)
        time.sleep(random.uniform(micro_pause_min, micro_pause_max))
        steps += 1
        if steps >= max_burst_steps:
            break

def wait_for_append_or_banner(driver, prev_count: int, timeout_ms: int) -> tuple[int, bool]:
    end = time.time() + timeout_ms / 1000.0
    while time.time() < end:
        if is_end_banner_visible(driver):
            return get_card_count(driver), True
        cur = get_card_count(driver)
        if cur > prev_count:
            return cur, False
        time.sleep(0.06 + random.random() * 0.06)
    return get_card_count(driver), is_end_banner_visible(driver)

# ---------------------- extraction & normalization ----------------------

ID_RX = re.compile(r"/product/(\d+)")

def extract_product_urls(driver, base: str) -> List[str]:
    paths = driver.execute_script("""
        const out = new Set();
        for (const el of document.querySelectorAll('[onclick*="getDetail("]')) {
            const s = el.getAttribute('onclick') || '';
            const m = s.match(/getDetail\\(\\s*['"]([^'"]+)['"]/i);
            if (m && m[1] && m[1].includes('/product/')) out.add(m[1]);
        }
        for (const a of document.querySelectorAll('a[href*="/product/"]')) {
            const href = a.getAttribute('href') || '';
            if (href) out.add(href);
        }
        for (const d of document.querySelectorAll('div[id^="product-"][data-id]')) {
            const id = (d.getAttribute('data-id') || '').trim();
            if (id) out.add(`/product/${id}`);
        }
        return Array.from(out);
    """) or []

    from urllib.parse import urlsplit, urlunsplit
    base_parts = urlsplit(base)
    urls = set()
    for p in paths:
        if not p:
            continue
        u = p if p.startswith("http") else (base + p)
        m = ID_RX.search(urlsplit(u).path)
        if not m:
            continue
        prod_id = m.group(1)
        canonical = urlunsplit((base_parts.scheme, base_parts.netloc, f"/product/{prod_id}", "", ""))
        urls.add(canonical)
    return sorted(urls)

# ---------------------- per-category scraping ----------------------

def scrape_category_bottom_blaster(driver, url: str,
                                   max_time: int, loops: int, stall_rounds: int,
                                   min_delta: int, max_delta: int,
                                   burst_steps: int,
                                   micro_pause_min: float, micro_pause_max: float,
                                   append_wait_min_ms: int, append_wait_max_ms: int) -> tuple[List[str], bool]:
    driver.get(url)
    _ensure_awake_and_viewport(driver)
    _install_scroll_helpers(driver)
    try:
        driver.execute_script("if (window.__scroll) window.__scroll.by(-1e9);")
    except Exception:
        pass

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.box.solid, div[id^='product-']"))
        )
    except Exception:
        pass

    start = time.time()
    prev_count = get_card_count(driver)
    stalled = 0
    reached_banner = False

    for _ in range(loops):
        _ensure_awake_and_viewport(driver)

        if (time.time() - start) > max_time:
            break
        if is_end_banner_visible(driver):
            reached_banner = True
            break

        blast_to_bottom_once(driver, burst_steps, min_delta, max_delta, micro_pause_min, micro_pause_max)
        wheel_scroll_from_element(driver, None, 20)
        _cdp_wheel(driver, 20)
        time.sleep(random.uniform(0.05, 0.12))

        new_count, banner = wait_for_append_or_banner(
            driver, prev_count, random.randint(append_wait_min_ms, append_wait_max_ms)
        )
        if banner:
            reached_banner = True
            break

        if new_count <= prev_count:
            stalled += 1
            if stalled >= max(1, stall_rounds):
                break
        else:
            stalled = 0
            prev_count = new_count

    if not reached_banner:
        reached_banner = is_end_banner_visible(driver)

    urls = extract_product_urls(driver, base_of(url))
    return urls, reached_banner

# ---------------------- categories loader ----------------------

def load_default_myvipon_categories() -> list[dict[str, str]]:
    """
    Loads categories from:
      1) settings.myvipon_categories_path if set
      2) scrapers/data/myvipon_categories.json
    Returns [{"name": "...", "url": "..."}].
    """
    if settings.myvipon_categories_path:
        p = Path(settings.myvipon_categories_path)
    else:
        p = Path(__file__).parent / "data" / "myvipon_categories.json"

    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("myvipon_categories.json must be a list of {name,url}")
    out: list[dict[str, str]] = []
    for row in data:
        name = str(row.get("name", "")).strip()
        url  = str(row.get("url", "")).strip()
        if name and url:
            out.append({"name": name, "url": url})
    if not out:
        raise ValueError("No valid categories in myvipon_categories.json")
    return out

# ---------------------- public entrypoint ----------------------

def collect_myvipon_urls(
    *,
    categories: list[dict[str, str]] | None = None,
    headed: bool = False,
    max_time: int = 600,
    loops: int = 800,
    stall_rounds: int = 6,
    min_delta: int = 600,
    max_delta: int = 2200,
    burst_steps: int = 12,
    micro_pause_min: float = 0.02,
    micro_pause_max: float = 0.06,
    append_wait_min_ms: int = 500,
    append_wait_max_ms: int = 1400,
    sleep_between: float = 1.0,
) -> dict:
    """
    Scrolls each category to bottom and returns:
      {
        "by_category": { "<name>": [urls...] },
        "all_urls": [unique urls across all categories]
      }
    """
    cats = categories or load_default_myvipon_categories()

    chrome_opts = Options()
    if headed:
        chrome_opts.add_argument("--window-size=1366,1000")
    else:
        chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--window-size=1366,1000")

    chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--force-device-scale-factor=1")
    chrome_opts.add_argument("--disable-background-timer-throttling")
    chrome_opts.add_argument("--disable-backgrounding-occluded-windows")
    chrome_opts.add_argument("--disable-renderer-backgrounding")
    chrome_opts.add_argument("--disable-features=CalculateNativeWinOcclusion")

    driver = uc.Chrome(options=chrome_opts)
    driver.set_page_load_timeout(60)

    by_category: Dict[str, List[str]] = {}
    all_set: set[str] = set()

    try:
        for cat in cats:
            name = cat["name"]
            url  = cat["url"]

            attempt = 1
            MAX_RETRIES = 3
            urls: List[str] = []

            while attempt <= MAX_RETRIES:
                urls, reached_banner = scrape_category_bottom_blaster(
                    driver, url,
                    max_time=max_time,
                    loops=loops,
                    stall_rounds=stall_rounds,
                    min_delta=min_delta,
                    max_delta=max_delta,
                    burst_steps=burst_steps,
                    micro_pause_min=micro_pause_min,
                    micro_pause_max=micro_pause_max,
                    append_wait_min_ms=append_wait_min_ms,
                    append_wait_max_ms=append_wait_max_ms,
                )
                if reached_banner:
                    break
                attempt += 1
                time.sleep(2)

            by_category[name] = urls
            all_set.update(urls)
            time.sleep(sleep_between)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return {"by_category": by_category, "all_urls": sorted(all_set)}
