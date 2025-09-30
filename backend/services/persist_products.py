# services/persist_products.py
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Dict, Any
from collections import defaultdict

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func, Numeric

import models

# Does your Product model have a price_value column?
HAS_PRICE_VALUE = hasattr(models.Product, "price_value")
HAS_STORE_FIELDS = all([
    hasattr(models.Product, "amazon_store_name"),
    hasattr(models.Product, "amazon_store_url"),
])

# Is Product.price a NUMERIC in the DB (vs TEXT/VARCHAR)?
PRICE_IS_NUMERIC = isinstance(models.Product.__table__.c.price.type, Numeric)
def _run_upsert_batch(db: Session, rows: list[dict]):
    """Upsert a homogeneous batch (all dicts share the same keys)."""
    if not rows:
        return 0

    stmt = insert(models.Product).values(rows)
    ex = stmt.excluded

    # Build SET map based on which columns are present in this batch
    present = set(rows[0].keys())

    set_map = {
        "type": func.coalesce(ex.type, models.Product.type) if "type" in present else models.Product.type,
        "category": func.coalesce(ex.category, models.Product.category) if "category" in present else models.Product.category,
        "updated_at": func.now(),
        "last_seen_at": func.now(),
    }
    if "price" in present:
        set_map["price"] = func.coalesce(ex.price, models.Product.price)
    if HAS_PRICE_VALUE and "price_value" in present:
        set_map["price_value"] = func.coalesce(ex.price_value, models.Product.price_value)
    if "title" in present:
        set_map["title"] = func.coalesce(ex.title, models.Product.title)
    if "image_url" in present:
        set_map["image_url"] = func.coalesce(ex.image_url, models.Product.image_url)
    if "description" in present:
        set_map["description"] = func.coalesce(ex.description, models.Product.description)
    if "amazon_url" in present:
        set_map["amazon_url"] = func.coalesce(ex.amazon_url, models.Product.amazon_url)
    if hasattr(models.Product, "amazon_store_name") and "amazon_store_name" in present:
        set_map["amazon_store_name"] = func.coalesce(ex.amazon_store_name, models.Product.amazon_store_name)
    if hasattr(models.Product, "amazon_store_url") and "amazon_store_url" in present:
        set_map["amazon_store_url"] = func.coalesce(ex.amazon_store_url, models.Product.amazon_store_url)

    stmt = stmt.on_conflict_do_update(
        index_elements=[models.Product.product_url],
        set_=set_map,
    )
    res = db.execute(stmt)
    return int(res.rowcount or 0)


# ----------------------- helpers -----------------------

def _normalize_url(u: str) -> str:
    """Simple URL normalization to reduce dupes."""
    return (u or "").strip().rstrip("/")

_price_keep_rx = re.compile(r"[^\d.\-]")

def _to_decimal(price_str: str | None) -> Decimal | None:
    """Convert '$1,299.50' -> Decimal('1299.50'); return None if not parseable."""
    if not price_str:
        return None
    s = _price_keep_rx.sub("", str(price_str))  # strip $, commas, spaces, etc.
    try:
        return Decimal(s) if s else None
    except InvalidOperation:
        return None

def _normalize_price_for_db(raw_price: Any):
    """
    Return a DB-ready value for products.price depending on its type:
    - If NUMERIC -> Decimal or None
    - If TEXT/VARCHAR -> original string or None
    """
    if raw_price is None or raw_price == "":
        return None
    if PRICE_IS_NUMERIC:
        return _to_decimal(raw_price)
    # keep pretty string (e.g., "$12.99") when price column is TEXT
    return str(raw_price)


# ----------------------- upserts -----------------------

def upsert_product_urls(
    db: Session,
    site_name: str,
    urls: List[str],
    ptype: Optional[str] = None,  # e.g., "coupon" | "rebate"
) -> dict:
    """
    Bulk upsert of product URLs for a given site.
    - product_url: unique (NOT NULL)
    - site_id: required
    - type: optional; if provided, it will be set/updated on conflict
    """
    site = db.query(models.Site).filter(models.Site.name == site_name).one()

    clean_urls = [_normalize_url(u) for u in urls if u]
    rows = [
        {
            "site_id": site.id,
            "product_url": u,
            **({"type": ptype} if ptype else {}),
        }
        for u in clean_urls
    ]

    if not rows:
        return {"inserted_or_updated": 0, "total_processed": 0}

    stmt = insert(models.Product).values(rows)

    set_map = {
        "site_id": site.id,
        "last_seen_at": func.now(),
        "updated_at": func.now(),
    }
    if ptype:
        set_map["type"] = ptype

    stmt = stmt.on_conflict_do_update(
        index_elements=[models.Product.product_url],
        set_=set_map,
    )

    result = db.execute(stmt)
    db.commit()

    return {
        "inserted_or_updated": int(result.rowcount or 0),
        "total_processed": len(rows),
    }


def upsert_product_items(db: Session, site_name: str, items: List[Dict[str, Any]]) -> dict:
    site = db.query(models.Site).filter(models.Site.name == site_name).one()

    raw_rows: list[dict] = []
    for it in items or []:
        u = _normalize_url(it.get("url") or it.get("product_url") or "")
        if not u:
            continue

        price_norm = _normalize_price_for_db(it.get("price"))
        price_value = (_to_decimal(it.get("price_value"))
                       if it.get("price_value") is not None
                       else _to_decimal(it.get("price")))

        row: Dict[str, Any] = {
            "site_id": site.id,
            "product_url": u,
            "type": it.get("type"),
            "category": it.get("category_name") or it.get("category"),
            "last_seen_at": func.now(),
            "updated_at": func.now(),
        }
        if price_norm is not None:
            row["price"] = price_norm
        if HAS_PRICE_VALUE and (price_value is not None):
            row["price_value"] = price_value

        raw_rows.append(row)

    if not raw_rows:
        return {"processed": 0, "affected": 0}

    # Group rows by identical key-set to avoid heterogeneous INSERTs
    groups: dict[tuple[str, ...], list[dict]] = defaultdict(list)
    for r in raw_rows:
        key = tuple(sorted(r.keys()))
        groups[key].append(r)

    affected = 0
    for _, batch in groups.items():
        affected += _run_upsert_batch(db, batch)

    db.commit()
    return {"processed": len(raw_rows), "affected": affected}


def upsert_product_details(db: Session, site_name: str, items: List[Dict[str, Any]]) -> dict:
    site = db.query(models.Site).filter(models.Site.name == site_name).one()

    rows = []
    any_has_price = False  # <-- track if any row provides a DB-ready price
    for it in items or []:
        u = str(it.get("url") or "").strip()
        if not u:
            continue

        price_norm = _normalize_price_for_db(it.get("price"))
        price_value = _to_decimal(it.get("price_value")) if it.get("price_value") is not None else _to_decimal(it.get("price"))

        row = {
            "site_id": site.id,
            "product_url": u,
            "title": it.get("title") or None,
            "image_url": it.get("image_url") or None,
            "description": it.get("description") or None,
            "category": it.get("category") or None,
            "amazon_url": it.get("amazon_url") or None,
            "updated_at": func.now(),
            "last_seen_at": func.now(),
        }
        if price_norm is not None:
            row["price"] = price_norm
            any_has_price = True
        if HAS_PRICE_VALUE and (price_value is not None):
            row["price_value"] = price_value

        if HAS_STORE_FIELDS:
            if it.get("amazon_store_name") is not None:
                row["amazon_store_name"] = it.get("amazon_store_name")
            if it.get("amazon_store_url") is not None:
                row["amazon_store_url"] = it.get("amazon_store_url")

        rows.append(row)

    if not rows:
        return {"processed": 0, "affected": 0}

    stmt = insert(models.Product).values(rows)
    ex = stmt.excluded

    set_map = {
        "title": func.coalesce(ex.title, models.Product.title),
        "image_url": func.coalesce(ex.image_url, models.Product.image_url),
        "description": func.coalesce(ex.description, models.Product.description),
        "category": func.coalesce(ex.category, models.Product.category),
        "amazon_url": func.coalesce(ex.amazon_url, models.Product.amazon_url),
        "updated_at": func.now(),
        "last_seen_at": func.now(),
    }
    if any_has_price:
        set_map["price"] = func.coalesce(ex.price, models.Product.price)
    if HAS_PRICE_VALUE:
        set_map["price_value"] = func.coalesce(ex.price_value, models.Product.price_value)
    if HAS_STORE_FIELDS:
        set_map["amazon_store_name"] = func.coalesce(ex.amazon_store_name, models.Product.amazon_store_name)
        set_map["amazon_store_url"]  = func.coalesce(ex.amazon_store_url,  models.Product.amazon_store_url)

    stmt = stmt.on_conflict_do_update(
        index_elements=[models.Product.product_url],
        set_=set_map,
    )
    res = db.execute(stmt)
    db.commit()
    return {"processed": len(rows), "affected": int(res.rowcount or 0)}


def upsert_amazon_store_fields(db: Session, site_name: str, items: List[Dict[str, Any]]) -> dict:
    """
    Lightweight updater: only (product_url, amazon_store_name, amazon_store_url).
    items: [{url, amazon_store_name, amazon_store_url}, ...]
    """
    if not HAS_STORE_FIELDS:
        return {"processed": 0, "affected": 0, "note": "Product model has no store fields"}

    site = db.query(models.Site).filter(models.Site.name == site_name).one()

    rows = []
    for it in items or []:
        u = str(it.get("url") or "").strip()
        if not u:
            continue
        rows.append({
            "site_id": site.id,
            "product_url": u,
            "amazon_store_name": it.get("amazon_store_name"),
            "amazon_store_url": it.get("amazon_store_url"),
            "updated_at": func.now(),
            "last_seen_at": func.now(),
        })

    if not rows:
        return {"processed": 0, "affected": 0}

    stmt = insert(models.Product).values(rows)
    ex = stmt.excluded

    stmt = stmt.on_conflict_do_update(
        index_elements=[models.Product.product_url],
        set_={
            "amazon_store_name": func.coalesce(ex.amazon_store_name, models.Product.amazon_store_name),
            "amazon_store_url":  func.coalesce(ex.amazon_store_url,  models.Product.amazon_store_url),
            "updated_at": func.now(),
            "last_seen_at": func.now(),
        },
    )
    res = db.execute(stmt)
    db.commit()
    return {"processed": len(rows), "affected": int(res.rowcount or 0)}