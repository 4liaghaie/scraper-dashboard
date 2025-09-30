# routers/metrics.py
from __future__ import annotations
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, Date

from db import get_session
import models

router = APIRouter(prefix="/metrics", tags=["metrics"])

@router.get("/products/by-site")
def products_by_site(db: Session = Depends(get_session)):
    rows = (
        db.query(
            models.Site.name.label("site"),
            func.count(models.Product.id).label("count"),
        )
        .join(models.Product, models.Product.site_id == models.Site.id)
        .group_by(models.Site.name)
        .order_by(models.Site.name.asc())
        .all()
    )
    return [{"site": r.site, "count": int(r.count)} for r in rows]

@router.get("/products/store-info")
def products_store_info(db: Session = Depends(get_session)):
    total = db.query(models.Product.id).count()
    with_amz = (
        db.query(models.Product.id)
        .filter(models.Product.amazon_url.isnot(None), models.Product.amazon_url != "")
        .count()
    )
    with_store = (
        db.query(models.Product.id)
        .filter(
            models.Product.amazon_store_name.isnot(None),
            models.Product.amazon_store_name != "",
            models.Product.amazon_store_url.isnot(None),
            models.Product.amazon_store_url != "",
        )
        .count()
    )
    missing_store = (
        db.query(models.Product.id)
        .filter(
            models.Product.amazon_url.isnot(None),
            models.Product.amazon_url != "",
            (
                (models.Product.amazon_store_name.is_(None))
                | (models.Product.amazon_store_name == "")
                | (models.Product.amazon_store_url.is_(None))
                | (models.Product.amazon_store_url == "")
            ),
        )
        .count()
    )
    return {
        "total": int(total),
        "with_amazon_url": int(with_amz),
        "with_store_info": int(with_store),
        "missing_store_info": int(missing_store),
    }

@router.get("/products/daily-new")
def products_daily_new(
    days: int = Query(14, ge=1, le=90), db: Session = Depends(get_session)
):
    since = datetime.utcnow() - timedelta(days=days)
    day_col = cast(models.Product.first_seen_at, Date).label("day")
    rows = (
        db.query(day_col, func.count(models.Product.id).label("count"))
        .filter(models.Product.first_seen_at >= since)
        .group_by(day_col)
        .order_by(day_col.asc())
        .all()
    )
    return [{"day": r.day.isoformat(), "count": int(r.count)} for r in rows]

@router.get("/jobs/status-counts")
def jobs_status_counts(db: Session = Depends(get_session)):
    rows = (
        db.query(models.JobRun.status, func.count(models.JobRun.id))
        .group_by(models.JobRun.status)
        .all()
    )
    return [{"status": s, "count": int(c)} for (s, c) in rows]

@router.get("/jobs/recent")
def jobs_recent(limit: int = Query(20, ge=1, le=200), db: Session = Depends(get_session)):
    runs = (
        db.query(models.JobRun)
        .order_by(models.JobRun.id.desc())
        .limit(limit)
        .all()
    )
    out = []
    for r in runs:
        dur = None
        if r.started_at and r.finished_at:
            dur = (r.finished_at - r.started_at).total_seconds()
        out.append(
            {
                "id": r.id,
                "job_id": r.job_id,
                "status": r.status,
                "queued_at": r.queued_at,
                "started_at": r.started_at,
                "finished_at": r.finished_at,
                "total": r.total,
                "processed": r.processed,
                "ok_count": r.ok_count,
                "fail_count": r.fail_count,
                "duration_s": dur,
                "note": r.note,
            }
        )
    return out
