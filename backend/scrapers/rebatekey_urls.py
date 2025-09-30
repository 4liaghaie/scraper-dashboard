from playwright.sync_api import sync_playwright

JS_COLLECT_URLS = """
(() => {
  const out = [];
  const cards = document.querySelectorAll('[data-e2e="listing-card"] .listing');
  for (const card of cards) {
    let a = card.querySelector('h3.title a[href]')
         || card.querySelector('a.preview[href]')
         || card.querySelector('a[href*="/coupon/"], a[href*="/rebate/"]');
    if (!a) continue;
    try {
      const href = new URL(a.getAttribute('href'), location.href).href.split('?')[0];
      out.push(href);
    } catch(e) {}
  }
  return out;
})();
"""

JS_DONE = r"""
(() => {
  const txt = (document.body.innerText || "").toLowerCase();
  return /\bno\s*more\s*deals\b/.test(txt) || /\bno\s*more\s*debates\b/.test(txt);
})();
"""

def _scrape_one(page, url: str):
  page.goto(url, wait_until="domcontentloaded")
  try:
    page.wait_for_selector('[data-e2e="listing-card"]', timeout=10000)
  except Exception:
    pass

  seen = set()
  ordered = []
  last_count = 0
  no_growth = 0

  for _ in range(3000):  # safety cap
    for h in page.evaluate(JS_COLLECT_URLS):
      key = h.rstrip("/")
      if key not in seen:
        seen.add(key)
        ordered.append(h)

    if page.evaluate(JS_DONE):
      break

    if len(ordered) == last_count:
      no_growth += 1
    else:
      no_growth = 0
      last_count = len(ordered)

    if no_growth >= 12:
      break

    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(900)

  return ordered

def collect_rebatekey_urls(headless: bool = True) -> dict:
  coupons_url = "https://rebatekey.com/coupons"
  rebates_url = "https://rebatekey.com/rebates"

  with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=headless)
    ctx = browser.new_context(viewport={"width": 1280, "height": 900})

    page = ctx.new_page()
    coupons_list = _scrape_one(page, coupons_url)

    page = ctx.new_page()
    rebate_list = _scrape_one(page, rebates_url)

    browser.close()

  return {
    "rebate_urls": rebate_list,
    "coupons_urls": coupons_list,  # keep the same key name if you like
  }
