# routers/exports.py
from __future__ import annotations

import csv
from io import StringIO
from typing import Iterable, List, Optional
from datetime import datetime, date, time

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import and_

from db import get_session
import models

router = APIRouter(prefix="/exports", tags=["exports"])

CSV_FIELDS = [
    "id",
    "site_id",
    "product_url",
    "type",
    "title",
    "price",
    "image_url",
    "description",
    "category",
    "amazon_url",
    "amazon_store_url",
    "amazon_store_name",
    "external_id",
    "first_seen_at",
    "last_seen_at",
    "created_at",
    "updated_at",
]

def _parse_date_bound(s: Optional[str], *, end: bool = False) -> Optional[datetime]:
    """
    Accepts:
      - 'YYYY-MM-DD' (date-only; expands to start/end of day)
      - full ISO-8601 'YYYY-MM-DDTHH:MM:SS' (naive ok)
    Returns naive datetime (assumes DB is UTC-naive).
    """
    if not s:
        return None
    try:
        if len(s) == 10:
            d = datetime.strptime(s, "%Y-%m-%d").date()
            return datetime.combine(d, time.max if end else time.min)
        return datetime.fromisoformat(s)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD or ISO-8601.")

def _row_from_product(p: models.Product) -> List[str]:
    # Convert values to strings, keep empty for None, ISO for datetimes
    def dt(v): return v.isoformat(timespec="seconds") if v else ""
    def s(v): return "" if v is None else str(v)

    return [
        s(p.id),
        s(p.site_id),
        s(p.product_url),
        s(p.type),
        s(p.title),
        s(p.price),
        s(p.image_url),
        s(p.description),
        s(p.category),
        s(p.amazon_url),
        s(p.amazon_store_url),
        s(p.amazon_store_name),
        s(p.external_id),
        dt(p.first_seen_at),
        dt(p.last_seen_at),
        dt(p.created_at),
        dt(p.updated_at),
    ]

def _stream_csv(rows: Iterable[models.Product]) -> Iterable[str]:
    """
    Stream CSV without loading all into memory.
    """
    buf = StringIO()
    writer = csv.writer(buf)
    # header
    writer.writerow(CSV_FIELDS)
    yield buf.getvalue()
    buf.seek(0); buf.truncate(0)

    for p in rows:
        writer.writerow(_row_from_product(p))
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)

def _base_query(db: Session):
    return db.query(models.Product)

@router.get("/products.csv", response_class=StreamingResponse)
def export_products_csv(
    db: Session = Depends(get_session),
    # filters
    site: Optional[str] = Query(None, description="Filter by site name (models.Site.name)"),
    site_id: Optional[int] = Query(None, description="Filter by site id"),
    last_seen_from: Optional[str] = Query(None, description="YYYY-MM-DD or ISO-8601"),
    last_seen_to: Optional[str] = Query(None, description="YYYY-MM-DD or ISO-8601"),
    type: Optional[str] = Query(None, description="Filter by Product.type"),
    # by number / slicing
    id: Optional[int] = Query(None, description="Export a single product by id"),
    ids: Optional[str] = Query(None, description="Comma-separated product ids, e.g. 1,2,3"),
    limit: Optional[int] = Query(None, ge=1, le=200000, description="Limit number of rows"),
):
    """
    Export products as CSV.

    Examples:
    - /exports/products.csv                          -> all rows
    - /exports/products.csv?site=rebatekey           -> filter by site name
    - /exports/products.csv?site_id=2                -> filter by site id
    - /exports/products.csv?last_seen_from=2025-09-20&last_seen_to=2025-10-02
    - /exports/products.csv?id=123                   -> single row by number (id)
    - /exports/products.csv?ids=10,11,12            -> selected rows by numbers (ids)
    - /exports/products.csv?limit=500                -> first N rows
    """
    q = _base_query(db)

    # Join Site only if filtering by name
    if site:
        q = q.join(models.Site).filter(models.Site.name == site)
    if site_id:
        q = q.filter(models.Product.site_id == site_id)
    if type:
        q = q.filter(models.Product.type == type)

    # by number
    selected_ids: List[int] = []
    if id is not None:
        selected_ids = [id]
    elif ids:
        try:
            selected_ids = [int(x.strip()) for x in ids.split(",") if x.strip()]
        except ValueError:
            raise HTTPException(status_code=400, detail="ids must be comma-separated integers")

    if selected_ids:
        q = q.filter(models.Product.id.in_(selected_ids))

    # last_seen_at range
    start_dt = _parse_date_bound(last_seen_from, end=False)
    end_dt = _parse_date_bound(last_seen_to, end=True)
    if start_dt and end_dt:
        if start_dt > end_dt:
            raise HTTPException(status_code=400, detail="last_seen_from must be <= last_seen_to")
        q = q.filter(and_(models.Product.last_seen_at >= start_dt,
                          models.Product.last_seen_at <= end_dt))
    elif start_dt:
        q = q.filter(models.Product.last_seen_at >= start_dt)
    elif end_dt:
        q = q.filter(models.Product.last_seen_at <= end_dt)

    # deterministic ordering for streaming
    q = q.order_by(models.Product.id.asc())

    if limit:
        q = q.limit(limit)

    # Use yield_per to keep memory low on large exports
    rows = q.yield_per(1000)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"products_{timestamp}.csv"

    return StreamingResponse(
        _stream_csv(rows),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

@router.get("/products/{product_id}.csv", response_class=StreamingResponse)
def export_single_product_csv(
    product_id: int,
    db: Session = Depends(get_session),
):
    """
    Convenience endpoint: export exactly one row by its numeric id.
    """
    p = db.query(models.Product).filter(models.Product.id == product_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Product not found")

    def one():
        buf = StringIO()
        w = csv.writer(buf)
        w.writerow(CSV_FIELDS)
        w.writerow(_row_from_product(p))
        yield buf.getvalue()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"product_{product_id}_{timestamp}.csv"
    return StreamingResponse(
        one(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
