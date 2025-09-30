# routers/product_actions.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy.orm import Session

from db import get_session
import models
from scrapers.amazon_store import scrape_amazon_store_many  # uses your updated version w/ diagnostics

router = APIRouter(prefix="/products", tags=["products"])

@router.post("/{product_id}/refresh-amazon-store")
def refresh_amazon_store(
    product_id: int = Path(..., ge=1),
    timeout_ms: int = Query(12000, ge=1000, le=60000),
    db: Session = Depends(get_session),
):
    """
    Scrape the product's amazon_url and update amazon_store_name/url for this single product.
    Returns diagnostics including anti-bot counts.
    """
    product = db.query(models.Product).filter(models.Product.id == product_id).first()
    if not product:
        raise HTTPException(404, "Product not found")
    if not (product.amazon_url or "").strip():
        raise HTTPException(400, "Product has no amazon_url")

    # Call scraper for just this URL, with diagnostics enabled
    store_map, stats = scrape_amazon_store_many(
        [product.amazon_url],
        timeout_ms=timeout_ms,
        concurrency=4,              # small concurrency is fine for single URL
        return_diagnostics=True,    # <-- enables anti-bot stats from your updated scraper
    )

    info = store_map.get(product.amazon_url) or {}
    name = (info.get("amazon_store_name") or "").strip()
    url  = (info.get("amazon_store_url") or "").strip()

    updated = False
    if name or url:
        # Update the single row directly
        product.amazon_store_name = name or product.amazon_store_name
        product.amazon_store_url  = url  or product.amazon_store_url
        db.add(product)
        db.commit()
        db.refresh(product)
        updated = True

    return {
        "product_id": product.id,
        "amazon_url": product.amazon_url,
        "found": bool(name or url),
        "amazon_store_name": name,
        "amazon_store_url": url,
        "updated": updated,
        # Diagnostics so you can see anti-bot behavior on this single call
        "antibot_hits": stats["antibot_hits"],
        "timeouts": stats["timeouts"],
        "http_errors": stats["http_errors"],
        "no_store_found": stats["no_store_found"],
        "antibot_sample": stats["antibot_urls"][:3],
    }
