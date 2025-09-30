from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from db import get_session
from scrapers.rebatekey_urls import collect_rebatekey_urls
from services.persist_products import upsert_product_urls, upsert_product_details, upsert_amazon_store_fields
from scrapers.rebaid_details import scrape_rebaid_details
from scrapers.amazon_store import scrape_amazon_store_many
from scrapers.rebatekey_details import collect_rebatekey_details

from services.persist_products import upsert_product_items
from scrapers.rebaid_urls import collect_rebaid_urls, load_default_rebaid_categories
from scrapers.myvipon_urls import collect_myvipon_urls, load_default_myvipon_categories
import models
from security import require_role
from sqlalchemy import or_, and_
from scrapers.myvipon_details import scrape_details_for_urls
from security import get_current_user
from schemas import RebaidScrapeIn

router = APIRouter(prefix="/scrape", tags=["scrape"])

@router.post("/rebatekey/urls", dependencies=[Depends(get_current_user)])
def scrape_rebatekey_urls(
    headed: bool = Query(False, description="Run with a visible browser window"),
    db: Session = Depends(get_session),
):
    """
    Scrape RebateKey list pages, then upsert product URLs into the DB.
    Assigns type='rebate' for /rebates results and type='coupon' for /coupons results.
    """
    data = collect_rebatekey_urls(headless=not headed)

    # Persist both lists under the 'rebatekey' site with explicit types
    res_rebates = upsert_product_urls(db, "rebatekey", data["rebate_urls"], ptype="rebate")
    res_coupons = upsert_product_urls(db, "rebatekey", data["coupons_urls"], ptype="coupon")

    return {
        "rebate_urls_count": len(data["rebate_urls"]),
        "coupons_urls_count": len(data["coupons_urls"]),
        "db": {
            "rebates": res_rebates,
            "coupons": res_coupons,
        },
        "sample": {
            "rebate": data["rebate_urls"][:3],
            "coupon": data["coupons_urls"][:3],
        },
    }
@router.post("/rebaid/urls", dependencies=[Depends(require_role("admin","superuser"))])
def run_rebaid_urls(
    db: Session = Depends(get_session),
    max_pages: int = Query(0, description="0 = all pages found"),
    timeout_ms: int = Query(30000),
    delay_min: float = Query(0.15),
    delay_max: float = Query(0.45),
):
    # 1) Load categories from local JSON file
    categories = load_default_rebaid_categories()

    # 2) Scrape
    data = collect_rebaid_urls(
        categories=categories,
        max_pages=max_pages,
        timeout_ms=timeout_ms,
        delay_min=delay_min,
        delay_max=delay_max,
    )

    # 3) Flatten & persist with product.type and category
    def tag(bucket: str):
        items = data.get(bucket, [])
        return [{
            "url": it["url"],
            "price": it.get("price",""),
            "price_value": it.get("price_value"),   # ← add this
            "category_name": it.get("category_name",""),
            "type": bucket
        } for it in items]

    res_codes       = upsert_product_items(db, "rebaid", tag("codes"))
    res_cashback    = upsert_product_items(db, "rebaid", tag("cashback"))
    res_buyonrebaid = upsert_product_items(db, "rebaid", tag("buyonrebaid"))

    return {
        "counts": {k: len(v) for k, v in {
            "codes": data.get("codes", []),
            "cashback": data.get("cashback", []),
            "buyonrebaid": data.get("buyonrebaid", []),
        }.items()},
        "db": {
            "codes": res_codes,
            "cashback": res_cashback,
            "buyonrebaid": res_buyonrebaid,
        }
    }

# Optional: quick preview of what's in the JSON
@router.get("/rebaid/categories", dependencies=[Depends(require_role("admin","superuser"))])
def show_rebaid_categories():
    return {"categories": load_default_rebaid_categories()}


@router.get("/myvipon/categories", dependencies=[Depends(require_role("admin","superuser"))])
def myvipon_categories_preview():
    return {"categories": load_default_myvipon_categories()}

@router.post("/myvipon/urls", dependencies=[Depends(require_role("admin","superuser"))])
def run_myvipon_urls(
    headed: bool = Query(False, description="Run Chrome with a visible window"),
    db: Session = Depends(get_session),
):
    """
    Scroll all myvipon categories (from bundled JSON) and upsert URLs into products (site='myvipon').
    """
    result = collect_myvipon_urls(headed=headed)

    # Map to the generic upsert (no price yet at this stage)
    items = []
    for cat_name, urls in result["by_category"].items():
        for u in urls:
            items.append({
                "url": u,
                "price_value": None,       # stage 1 just finds URLs
                "type": None,              # myvipon has no 'codes/cashback' buckets
                "category_name": cat_name, # keep your input category name
            })

    db_res = upsert_product_items(db, "myvipon", items)

    return {
        "counts": {k: len(v) for k, v in result["by_category"].items()},
        "total": len(result["all_urls"]),
        "db": db_res,
        "sample": result["all_urls"][:5],
    }

@router.post("/myvipon/details", dependencies=[Depends(require_role("admin","superuser"))])
def scrape_myvipon_details(
    db: Session = Depends(get_session),
    limit: int = Query(200, ge=1, le=5000, description="Max URLs to scrape this run"),
    only_missing: bool = Query(True, description="Only rows without title/description"),
    workers: int = Query(6, ge=1, le=32),
    timeout: int = Query(30, ge=5, le=120),
    proxy: str | None = Query(None, description="Optional http(s) proxy URL"),
):
    # 1) Gather target URLs from DB
    site = db.query(models.Site).filter(models.Site.name == "myvipon").one_or_none()
    if not site:
        raise HTTPException(400, "Site 'myvipon' is not seeded")

    q = db.query(models.Product.product_url).filter(models.Product.site_id == site.id)

    if only_missing:
        q = q.filter(or_(
            models.Product.title.is_(None),
            models.Product.title == "",
            models.Product.description.is_(None),
            models.Product.description == "",
        ))

    urls = [u for (u,) in q.order_by(models.Product.id.desc()).limit(limit).all()]
    if not urls:
        return {"selected": 0, "scraped": 0, "db": {"processed": 0, "affected": 0}}

    # 2) Scrape details
    scraped = scrape_details_for_urls(
        urls,
        workers=workers,
        timeout=timeout,
        proxy=proxy,
    )

    # 3) Persist back
    ok_items = [r for r in scraped if r.get("status") == "ok"]
    db_res = upsert_product_details(db, "myvipon", ok_items)

    errors = [r for r in scraped if r.get("status") == "error"]

    return {
        "selected": len(urls),
        "scraped": len(scraped),
        "ok": len(ok_items),
        "errors": len(errors),
        "db": db_res,
        "sample_ok": ok_items[:3],
        "sample_err": errors[:2],
    }
@router.post("/rebaid/details", dependencies=[Depends(require_role("admin","superuser"))])
def scrape_rebaid_details_endpoint(
    db: Session = Depends(get_session),
    missing_only: bool = Query(True, description="Only scrape products missing some details"),
    limit: int = Query(300, ge=1, le=5000),
    timeout_ms: int = Query(12000),
):
    """
    Scrape Rebaid product pages for title/description/amazon_url/image_url.
    Does *not* scrape Amazon store info (that’s a separate endpoint).
    """
    q = (
        db.query(models.Product)
        .join(models.Site)
        .filter(models.Site.name == "rebaid")
    )
    if missing_only:
        # missing any of these fields?
        q = q.filter(
            (models.Product.title.is_(None)) |
            (models.Product.description.is_(None)) |
            (models.Product.image_url.is_(None)) |
            (models.Product.amazon_url.is_(None))
        )
    q = q.order_by(models.Product.created_at.desc()).limit(limit)

    rows = q.all()
    urls = [r.product_url for r in rows if r.product_url]

    scraped = scrape_rebaid_details(urls, timeout_ms=timeout_ms)

    # Prepare items for upsert (site='rebaid')
    items = []
    for s in scraped:
        items.append({
            "url": s["url"],
            "title": s.get("title"),
            "description": s.get("description"),
            "amazon_url": s.get("amazon_url"),
            "image_url": s.get("image_url"),
            # don't pass "price" here (we keep the listing price already stored)
        })

    db_res = upsert_product_details(db, "rebaid", items)
    return {
        "requested": len(urls),
        "scraped": len(scraped),
        "db": db_res,
        "sample": scraped[:5],
    }

@router.post("/amazon/stores", dependencies=[Depends(require_role("admin","superuser"))])
def scrape_amazon_stores_endpoint(
    db: Session = Depends(get_session),
    site: str | None = Query(None, description="If provided, restrict to this site (e.g. 'rebaid', 'myvipon')"),
    missing_only: bool = Query(True, description="Only rows missing store fields"),
    limit: int = Query(500, ge=1, le=5000),
    timeout_ms: int = Query(12000),
):
    """
    Given products with `amazon_url`, fetch Amazon page and fill store name/url.
    Works for any site (rebaid, myvipon, rebatekey, etc.).
    """
    q = db.query(models.Product).join(models.Site)
    if site:
        q = q.filter(models.Site.name == site)
    if missing_only:
        q = q.filter(
            (models.Product.amazon_url.is_not(None)) &
            (models.Product.amazon_url != "") &
            (
              (models.Product.amazon_store_name.is_(None)) |
              (models.Product.amazon_store_name == "") |
              (models.Product.amazon_store_url.is_(None)) |
              (models.Product.amazon_store_url == "")
            )
        )
    else:
        q = q.filter(
            (models.Product.amazon_url.is_not(None)) &
            (models.Product.amazon_url != "")
        )
    q = q.order_by(models.Product.updated_at.desc()).limit(limit)

    rows = q.all()
    if not rows:
        return {"requested": 0, "scraped": 0, "db": {"processed": 0, "affected": 0}}

    urls = [r.amazon_url for r in rows if r.amazon_url]
    store_map, stats = scrape_amazon_store_many(
        urls,
        timeout_ms=timeout_ms,
        concurrency=16,          # keep or tune
        return_diagnostics=True  # <-- this enables anti-bot stats
    )

    # Map back to product_url for upsert
    items = []
    for r in rows:
        info = store_map.get(r.amazon_url or "")
        if not info:
            continue
        items.append({
            "url": r.product_url,
            "amazon_store_name": info.get("amazon_store_name"),
            "amazon_store_url": info.get("amazon_store_url"),
        })

    # Use lightweight updater (or upsert_product_details also works via COALESCE)
    site_name = rows[0].site.name if rows else (site or "unknown")
    db_res = upsert_amazon_store_fields(db, site_name, items)

    return {
        "requested": len(urls),
        "scraped": len(items),
        "db": db_res,
        "sample": items[:5],
        "antibot_hits": stats["antibot_hits"],
        "timeouts": stats["timeouts"],
        "http_errors": stats["http_errors"],
        "no_store_found": stats["no_store_found"],
        "antibot_sample": stats["antibot_urls"][:5],
        "sample": items[:5],
    }
@router.post("/rebatekey/details", dependencies=[Depends(require_role("admin","superuser"))])
def scrape_rebatekey_details_endpoint(
    db: Session = Depends(get_session),
    missing_only: bool = Query(True, description="Only rows missing some details"),
    limit: int = Query(300, ge=1, le=5000),
    concurrency: int = Query(12, ge=1, le=64),
    retries: int = Query(2, ge=0, le=5),
    timeout: float = Query(20.0, ge=5.0, le=60.0),
):
    """
    Scrape RebateKey product detail pages (title, price, image_url, amazon_url, category, description).
    Amazon store enrichment is handled by /scrape/amazon/stores.
    """
    q = (
        db.query(models.Product)
        .join(models.Site)
        .filter(models.Site.name == "rebatekey")
    )
    if missing_only:
        q = q.filter(
            (models.Product.title.is_(None)) |
            (models.Product.description.is_(None)) |
            (models.Product.image_url.is_(None)) |
            (models.Product.amazon_url.is_(None)) |
            (models.Product.category.is_(None)) |
            (models.Product.price.is_(None))
        )
    q = q.order_by(models.Product.created_at.desc()).limit(limit)
    rows = q.all()
    urls = [r.product_url for r in rows if r.product_url]

    scraped = collect_rebatekey_details(
        urls,
        concurrency=concurrency,
        retries=retries,
        timeout=timeout,
    )

    # Map to DB upsert payload (site='rebatekey')
    items = []
    for s in scraped:
        items.append({
            "url": s["url"],
            "title": s.get("title"),
            "price": s.get("price"),           # TEXT-friendly, e.g. "$12.99"
            "image_url": s.get("image_url"),
            "description": s.get("description"),
            "category": s.get("category"),
            "amazon_url": s.get("amazon_url"),
        })

    db_res = upsert_product_details(db, "rebatekey", items)
    return {
        "requested": len(urls),
        "scraped": len(scraped),
        "db": db_res,
        "sample": scraped[:5],
    }