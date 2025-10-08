# routers/jobs.py
from __future__ import annotations
import json
from sqlalchemy import or_

import asyncio
from typing import Any, Dict, Optional, Callable, List
from datetime import datetime  # <-- added for DB persistence timestamps

from fastapi import APIRouter, Depends, HTTPException, Request, Query
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from sqlalchemy.orm import Session

from db import get_session, SessionLocal
from deps import job_manager
import models
from functools import partial
# --- Scrapers (sync functions) ---
from scrapers.rebaid_details import scrape_rebaid_details  # bulk, sync
from scrapers.rebatekey_details import collect_rebatekey_details  # bulk, sync
from scrapers.amazon_store import scrape_amazon_store_many  # bulk, sync
from scrapers.rebaid_urls import collect_rebaid_urls, load_default_rebaid_categories
from scrapers.rebatekey_urls import collect_rebatekey_urls
from scrapers.myvipon_urls import collect_myvipon_urls
from scrapers.myvipon_details import _scrape_one as vipon_scrape_one, DEFAULT_REFERER as VIPON_REFERER
from scrapers.myvipon_details import scrape_details_for_urls as scrape_myvipon_details
from sqlalchemy.exc import DBAPIError, ProgrammingError

# --- Persistence (sync functions) ---
from services.persist_products import (
    upsert_product_details,
    upsert_amazon_store_fields,
    upsert_product_items,
    upsert_product_urls,
)

router = APIRouter(prefix="/jobs", tags=["jobs"])
JOB_TASKS: dict[str, asyncio.Task] = {}

# ---- Persist job_manager state to DB (place right after router = APIRouter(...)) ----
# This wraps job_manager so every create/mark_running/tick/finish also writes to your
# Job / JobRun / JobEvent tables. No changes to your existing code paths.
if not getattr(job_manager, "_db_persist_patched", False):
    _JOBID_TO_DB_RUN: Dict[str, int] = {}

    _orig_create = job_manager.create
    _orig_mark_running = job_manager.mark_running
    _orig_tick = job_manager.tick
    _orig_finish = job_manager.finish

    async def _persist_create(*, kind: str, total: int = 0, meta: Optional[dict] = None):
        st = await _orig_create(kind=kind, total=total, meta=meta or {})
        with SessionLocal() as db2:
            job = db2.query(models.Job).filter(models.Job.name == kind).one_or_none()
            if not job:
                job = models.Job(name=kind, is_active=True)
                db2.add(job)
                db2.flush()  # get job.id
            run = models.JobRun(
                job_id=job.id,
                status="queued",
                queued_at=datetime.utcnow(),
                total=int(total or 0),
                note=(meta or {}).get("note", ""),
                meta=(meta or {}),
            )
            db2.add(run)
            db2.commit()
            _JOBID_TO_DB_RUN[st.id] = run.id
            try:
                st.meta["db_run_id"] = run.id
            except Exception:
                pass
        return st

    async def _persist_mark_running(job_id: str, total: Optional[int] = None):
        res = await _orig_mark_running(job_id, total)
        run_id = _JOBID_TO_DB_RUN.get(job_id)
        if run_id:
            with SessionLocal() as db2:
                run = db2.query(models.JobRun).filter(models.JobRun.id == run_id).first()
                if run:
                    if total is not None:
                        run.total = int(total)
                    run.status = "running"
                    run.started_at = datetime.utcnow()
                    db2.commit()
        return res

    async def _persist_tick(job_id: str, *, ok: bool, plus: int = 0, note: str = "", meta: Optional[dict] = None):
        res = await _orig_tick(job_id, ok=ok, plus=plus, note=note, meta=meta)
        run_id = _JOBID_TO_DB_RUN.get(job_id)
        if not run_id:
            return res
        with SessionLocal() as db2:
            run = db2.query(models.JobRun).filter(models.JobRun.id == run_id).first()
            if run:
                inc = int(plus or 0)
                run.processed = (run.processed or 0) + (inc if inc > 0 else 1)
                if ok:
                    run.ok_count = (run.ok_count or 0) + (inc if inc > 0 else 1)
                    level = "info"
                else:
                    run.fail_count = (run.fail_count or 0) + (inc if inc > 0 else 1)
                    level = "error"
                db2.add(models.JobEvent(
                    run_id=run_id,
                    ts=datetime.utcnow(),
                    level=level,
                    message=note or "",
                    plus=inc if inc > 0 else 1,
                    meta=meta or {},
                ))
            db2.commit()
        return res

    async def _persist_finish(job_id: str, status: str, note: str = ""):
        res = await _orig_finish(job_id, status, note)
        run_id = _JOBID_TO_DB_RUN.get(job_id)
        if run_id:
            with SessionLocal() as db2:
                run = db2.query(models.JobRun).filter(models.JobRun.id == run_id).first()
                if run:
                    run.status = status
                    run.finished_at = datetime.utcnow()
                    if status == "error":
                        run.error_text = note
                    elif note:
                        run.note = note
                    db2.commit()
            _JOBID_TO_DB_RUN.pop(job_id, None)
        return res

    job_manager.create = _persist_create
    job_manager.mark_running = _persist_mark_running
    job_manager.tick = _persist_tick
    job_manager.finish = _persist_finish
    job_manager._db_persist_patched = True
# ---- end persistence shim ----


# ---------- status + SSE stream ----------
@router.get("/status/{job_id}")
def job_status(job_id: str):
    st = job_manager.get(job_id)
    if not st:
        raise HTTPException(404, "job not found")
    return st.__dict__


@router.get("/stream/{job_id}")
async def job_stream(job_id: str, request: Request):
    st = job_manager.get(job_id)
    if not st:
        raise HTTPException(404, "job not found")

    async def gen():
        async for event in job_manager.stream(job_id):
            if await request.is_disconnected():
                break
            if event.get("type") == "end":
                break
            # ⭐ Ensure valid JSON for the browser
            yield {
                "event": event["type"],
                "data": json.dumps(event["state"]),
            }

    return EventSourceResponse(gen())


# ---------- start a scrape job ----------
class StartScrapeIn(BaseModel):
    kind: str = Field(
        ...,
        description="rebaid_details | rebatekey_details | amazon_stores | rebaid_urls | rebatekey_urls | myvipon_urls | full_fresh_run",
    )
    params: Dict[str, Any] = Field(default_factory=dict)


@router.post("/start")        # canonical
async def start_scrape_job(payload: StartScrapeIn, db: Session = Depends(get_session)):
    return await _start_job(payload, db)

@router.post("/start/run")    # alias
async def start_scrape_job_alias(payload: StartScrapeIn, db: Session = Depends(get_session)):
    return await _start_job(payload, db)

@router.post("/scrape")       # legacy alias
async def start_scrape_job_compat(payload: StartScrapeIn, db: Session = Depends(get_session)):
    return await _start_job(payload, db)


async def _start_job(payload: StartScrapeIn, db: Session):
    kind = payload.kind
    if kind == "rebaid_details":
        total, run_coro = await _prep_rebaid_details(payload.params, db)
    elif kind == "rebatekey_details":
        total, run_coro = await _prep_rebatekey_details(payload.params, db)
    elif kind == "myvipon_details":
        total, run_coro = await _prep_myvipon_details(payload.params, db)
    elif kind == "amazon_stores":
        total, run_coro = await _prep_amazon_stores(payload.params, db)
    elif kind == "rebaid_urls":
        total, run_coro = await _prep_rebaid_urls(payload.params, db)
    elif kind == "rebatekey_urls":
        total, run_coro = await _prep_rebatekey_urls(payload.params, db)
    elif kind == "myvipon_urls":
        total, run_coro = await _prep_myvipon_urls(payload.params, db)
    elif kind == "full_fresh_run":                              # <— NEW
        total, run_coro = await _prep_full_fresh_run(payload.params, db)  # <— NEW
    else:
        raise HTTPException(400, "unknown kind")

    st = await job_manager.create(kind=kind, total=total, meta={"params": payload.params})
    task = asyncio.create_task(_run_job(st.id, run_coro))
    JOB_TASKS[st.id] = task
    task.add_done_callback(lambda t, jid=st.id: JOB_TASKS.pop(jid, None))

    return {"job_id": st.id, "kind": kind, "total": total}


async def _run_job(job_id: str, run_coro: Callable[[str], Any]):
    try:
        await job_manager.mark_running(job_id)
        await run_coro(job_id)
        await job_manager.finish(job_id, "done")
    except asyncio.CancelledError:
        # Mark as canceled and re-raise to stop the task immediately
        await job_manager.finish(job_id, "canceled", note="canceled by request")
        raise
    except Exception as e:
        await job_manager.finish(job_id, "error", note=str(e))

# ---------- job preparations (one per kind) ----------
async def _prep_rebaid_details(p: Dict[str, Any], db: Session):
    missing_only: bool = bool(p.get("missing_only", False))
    limit: int = int(p.get("limit", 1000))
    timeout_ms: int = int(p.get("timeout_ms", 12000))

    # Use request-scoped db *only* to discover targets
    q = db.query(models.Product).join(models.Site).filter(models.Site.name == "rebaid")
    if missing_only:
        q = q.filter(
            (models.Product.title.is_(None))
            | (models.Product.description.is_(None))
            | (models.Product.image_url.is_(None))
            | (models.Product.amazon_url.is_(None))
        )
    rows = q.order_by(models.Product.created_at.desc()).limit(limit).all()
    urls: List[str] = [r.product_url for r in rows if r.product_url]
    total = len(urls)

    async def run(job_id: str):
        # fresh DB session inside the background job
        with SessionLocal() as db2:
            ok_items: list[dict] = []
            # run each scrape in a thread to keep the loop free for SSE
            for u in urls:
                try:
                    data_list = await asyncio.to_thread(scrape_rebaid_details, [u], timeout_ms=timeout_ms)
                    data = data_list[0] if data_list else {"url": u}
                    ok_items.append(
                        {
                            "url": data.get("url", u),
                            "title": data.get("title"),
                            "description": data.get("description"),
                            "amazon_url": data.get("amazon_url"),
                            "image_url": data.get("image_url"),
                        }
                    )
                    await job_manager.tick(job_id, ok=True, meta={"last_url": u})
                except Exception as e:
                    await job_manager.tick(job_id, ok=False, note=f"{u}: {e}")

            if ok_items:
                await asyncio.to_thread(upsert_product_details, db2, "rebaid", ok_items)

    return total, run

async def _prep_myvipon_details(p: Dict[str, Any], db: Session):
    """
    Scrape MyVipon product detail pages and upsert into DB with progress.
    Params:
      - only_missing (bool, default True)
      - limit        (int,  default 200)
      - workers      (int,  default 6)      -> thread concurrency
      - timeout      (int,  default 30)     -> seconds
      - proxy        (str|None, default None)
      - retries      (int,  default 2)
      - backoff      (float,default 1.0)
      - referer      (str,  default VIPON_REFERER)
    """
    only_missing: bool = bool(p.get("only_missing", False))
    limit: int = int(p.get("limit", 200))
    workers: int = int(p.get("workers", 6))
    timeout: int = int(p.get("timeout", 30))
    proxy: Optional[str] = p.get("proxy")
    retries: int = int(p.get("retries", 2))
    backoff: float = float(p.get("backoff", 1.0))
    referer: str = p.get("referer") or VIPON_REFERER

    # discover targets using the request-scoped session
    site = db.query(models.Site).filter(models.Site.name == "myvipon").one_or_none()
    if not site:
        raise HTTPException(400, "Site 'myvipon' is not seeded")

    q = db.query(models.Product.product_url).filter(models.Product.site_id == site.id)
    if only_missing:
        q = q.filter(
            or_(
                models.Product.title.is_(None),
                models.Product.title == "",
                models.Product.description.is_(None),
                models.Product.description == "",
            )
        )

    urls = [u for (u,) in q.order_by(models.Product.id.desc()).limit(limit).all() if u]
    total = len(urls)

    async def run(job_id: str):
        # fresh DB session inside the background task
        from db import SessionLocal
        from services.persist_products import upsert_product_details

        await job_manager.mark_running(job_id, total)
        results: list[dict] = []

        # limit concurrency with a semaphore and run each scrape in a thread
        sem = asyncio.Semaphore(workers)

        async def run_one(u: str):
            async with sem:
                try:
                    data = await asyncio.to_thread(
                        vipon_scrape_one, u, referer, timeout, proxy, retries, backoff
                    )
                    ok = data.get("status") == "ok"
                    await job_manager.tick(job_id, ok=ok, meta={"last_url": u})
                    return data
                except Exception as e:
                    await job_manager.tick(job_id, ok=False, meta={"last_url": u}, note=str(e))
                    return {"url": u, "status": "error", "error": str(e)}

        tasks = [asyncio.create_task(run_one(u)) for u in urls]
        for fut in asyncio.as_completed(tasks):
            results.append(await fut)

        ok_items = [r for r in results if r.get("status") == "ok"]
        if ok_items:
            # upsert in a thread, using a short-lived session
            with SessionLocal() as db2:
                await asyncio.to_thread(upsert_product_details, db2, "myvipon", ok_items)

    return total, run

async def _prep_rebatekey_details(p: Dict[str, Any], db: Session):
    missing_only: bool = bool(p.get("missing_only", False))
    limit: int = int(p.get("limit", 300))
    concurrency: int = int(p.get("concurrency", 12))
    retries: int = int(p.get("retries", 2))
    timeout: float = float(p.get("timeout", 20.0))

    q = db.query(models.Product).join(models.Site).filter(models.Site.name == "rebatekey")
    if missing_only:
        q = q.filter(
            (models.Product.title.is_(None))
            | (models.Product.description.is_(None))
            | (models.Product.image_url.is_(None))
            | (models.Product.amazon_url.is_(None))
            | (models.Product.category.is_(None))
            | (models.Product.price.is_(None))
        )
    rows = q.order_by(models.Product.created_at.desc()).limit(limit).all()
    urls: List[str] = [r.product_url for r in rows if r.product_url]
    total = len(urls)

    async def run(job_id: str):
        with SessionLocal() as db2:
            loop = asyncio.get_running_loop()

            # Thread-safe progress callback (called from worker thread)
            def on_progress(idx: int, url: str, ok: bool, meta: dict | None = None):
                loop.call_soon_threadsafe(
                    asyncio.create_task,
                    job_manager.tick(job_id, ok=ok, meta={"last_url": url, **(meta or {})}),
                )

            # run the whole bulk scrape in a separate thread
            try:
                scraped = await asyncio.to_thread(
                    collect_rebatekey_details,
                    urls,
                    concurrency=concurrency,
                    retries=retries,
                    timeout=timeout,
                    on_progress=on_progress,  # safe now
                )
            except TypeError:
                scraped = await asyncio.to_thread(
                    collect_rebatekey_details,
                    urls,
                    concurrency=concurrency,
                    retries=retries,
                    timeout=timeout,
                )

            items = [
                {
                    "url": s["url"],
                    "title": s.get("title"),
                    "price": s.get("price"),
                    "image_url": s.get("image_url"),
                    "description": s.get("description"),
                    "category": s.get("category"),
                    "amazon_url": s.get("amazon_url"),
                }
                for s in scraped
            ]

            if items:
                await asyncio.to_thread(upsert_product_details, db2, "rebatekey", items)

    return total, run


async def _prep_amazon_stores(p: Dict[str, Any], db: Session):
    site: Optional[str] = p.get("site")
    missing_only: bool = bool(p.get("missing_only", True))
    limit: int = int(p.get("limit", 500))
    timeout_ms: int = int(p.get("timeout_ms", 12000))

    q = db.query(models.Product).join(models.Site)
    if site:
        q = q.filter(models.Site.name == site)
    if missing_only:
        q = q.filter(
            (models.Product.amazon_url.is_not(None))
            & (models.Product.amazon_url != "")
            & (
                (models.Product.amazon_store_name.is_(None))
                | (models.Product.amazon_store_name == "")
                | (models.Product.amazon_store_url.is_(None))
                | (models.Product.amazon_store_url == "")
            )
        )
    else:
        q = q.filter((models.Product.amazon_url.is_not(None)) & (models.Product.amazon_url != ""))

    rows = q.order_by(models.Product.updated_at.desc()).limit(limit).all()
    urls: List[str] = [r.amazon_url for r in rows if r.amazon_url]
    total = len(urls)

    async def run(job_id: str):
        with SessionLocal() as db2:
            BATCH = 25
            site_name = rows[0].site.name if rows else (site or "unknown")

            processed = 0
            for i in range(0, len(urls), BATCH):
                chunk = urls[i : i + BATCH]

                store_map = await asyncio.to_thread(
                    scrape_amazon_store_many, chunk, timeout_ms=timeout_ms
                )

                items: list[dict] = []
                # map results back to products in this batch only (minor perf)
                batch_urls = set(chunk)
                for r in rows:
                    if r.amazon_url in batch_urls and r.amazon_url in store_map:
                        info = store_map[r.amazon_url]
                        items.append(
                            {
                                "url": r.product_url,
                                "amazon_store_name": info.get("amazon_store_name"),
                                "amazon_store_url": info.get("amazon_store_url"),
                            }
                        )

                if items:
                    await asyncio.to_thread(
                        upsert_amazon_store_fields, db2, site_name, items
                    )

                processed += len(chunk)
                await job_manager.tick(job_id, ok=True, plus=len(chunk), meta={"last_batch_end": processed})

    return total, run


async def _prep_rebaid_urls(p: Dict[str, Any], db: Session):
    max_pages: int = int(p.get("max_pages", 0))      # 0 = all
    timeout_ms: int = int(p.get("timeout_ms", 30000))
    delay_min: float = float(p.get("delay_min", 0.15))
    delay_max: float = float(p.get("delay_max", 0.45))

    async def run(job_id: str):
# --- inside _prep_rebaid_urls() -> run() ---
        with SessionLocal() as db2:
            categories = load_default_rebaid_categories()

            # ✅ pass kwargs, not a single dict
            data = collect_rebaid_urls(
                categories=categories,
                max_pages=max_pages,
                timeout_ms=timeout_ms,
                delay_min=delay_min,
                delay_max=delay_max,
            )

            def tag(bucket: str):
                items = data.get(bucket, [])
                return [
                    {
                        "url": it["url"],
                        "price": it.get("price", ""),
                        "price_value": it.get("price_value"),
                        "category_name": it.get("category_name", ""),
                        "type": bucket,
                    }
                    for it in items
                ]

            if data.get("codes"):
                await asyncio.to_thread(upsert_product_items, db2, "rebaid", tag("codes"))
            if data.get("cashback"):
                await asyncio.to_thread(upsert_product_items, db2, "rebaid", tag("cashback"))
            if data.get("buyonrebaid"):
                await asyncio.to_thread(upsert_product_items, db2, "rebaid", tag("buyonrebaid"))

            await job_manager.tick(job_id, ok=True, note="rebaid urls completed")


    return 0, run


async def _prep_rebatekey_urls(p: Dict[str, Any], db: Session):
    headed: bool = bool(p.get("headed", False))

    async def run(job_id: str):
        with SessionLocal() as db2:
            data = await asyncio.to_thread(collect_rebatekey_urls, headless=not headed)

            if data.get("rebate_urls"):
                await asyncio.to_thread(
                    upsert_product_urls, db2, "rebatekey", data.get("rebate_urls", []), "rebate"
                )
            if data.get("coupons_urls"):
                await asyncio.to_thread(
                    upsert_product_urls, db2, "rebatekey", data.get("coupons_urls", []), "coupon"
                )

            await job_manager.tick(job_id, ok=True, note="rebatekey urls completed")

    return 0, run


async def _prep_myvipon_urls(p: Dict[str, Any], db: Session):
    headed: bool = bool(p.get("headed", False))

    async def run(job_id: str):
        with SessionLocal() as db2:
            result = await asyncio.to_thread(collect_myvipon_urls, headed=headed)

            items: list[dict] = []
            for cat_name, urls in result.get("by_category", {}).items():
                for u in urls:
                    items.append(
                        {"url": u, "price_value": None, "type": None, "category_name": cat_name}
                    )
            if items:
                await asyncio.to_thread(upsert_product_items, db2, "myvipon", items)

            await job_manager.tick(job_id, ok=True, note="myvipon urls completed")

    return 0, run

# routers/jobs.py (add near the other _prep_* functions)
async def _prep_full_fresh_run(p: Dict[str, Any], db: Session):
    """
    Full pipeline (NO Amazon store enrichment), with strict de-duplication:
      1) Collect fresh URLs from Rebaid/RebateKey/MyVipon (skip URLs already in DB)
      2) Scrape details for those new URLs (per site)
    """
    import asyncio
    from functools import partial
    from collections import OrderedDict
    from sqlalchemy.exc import DBAPIError, ProgrammingError

    # ----------------- small utils -----------------
    def _dialect_name_of(session) -> str:
        try:
            return (session.bind.dialect.name or "").lower()
        except Exception:
            return ""

    def _is_too_many_params(exc: Exception) -> bool:
        msg = str(getattr(exc, "orig", exc)).lower()
        return ("too many sql variables" in msg) or ("too many parameters" in msg) or ("f405" in msg)

    def _dedupe_urls(urls: list[str]) -> list[str]:
        seen = set()
        out = []
        for u in urls:
            if not u:
                continue
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    def _dedupe_items_by_url(items: list[dict], url_key: str = "url") -> list[dict]:
        """Merge duplicates by URL; prefer non-null/non-empty fields from later items."""
        merged: "OrderedDict[str, dict]" = OrderedDict()
        for it in items:
            u = it.get(url_key)
            if not u:
                # allow alt key name
                u = it.get("product_url")
                if u:
                    it[url_key] = u
            if not u:
                continue
            if u not in merged:
                merged[u] = it.copy()
            else:
                base = merged[u]
                # fill blanks only; do NOT overwrite non-empty values
                for k, v in it.items():
                    if k in ("url", "product_url"):
                        continue
                    if base.get(k) in (None, "", []):
                        if v not in (None, "", []):
                            base[k] = v
        return list(merged.values())

    async def _adaptive_upsert(call, rows: list, *, approx_cols_per_row: int, start_cap: int, session):
        """Split chunks if DB complains about parameter limits."""
        name = _dialect_name_of(session)
        if name == "sqlite":
            max_rows = max(1, 900 // max(1, approx_cols_per_row))
            bsz = min(start_cap, max_rows)
        else:
            bsz = start_cap

        i = 0
        n = len(rows)
        while i < n:
            chunk = rows[i:i + bsz]
            try:
                await asyncio.to_thread(call, chunk)
                i += len(chunk)
            except (ProgrammingError, DBAPIError) as e:
                if _is_too_many_params(e):
                    if bsz == 1:
                        raise
                    bsz = max(1, bsz // 2)
                    continue
                raise

    async def _chunked_upsert_items(session, site_name: str, items: list[dict]) -> None:
        if not items:
            return
        # global dedupe (pre-chunk)
        items = _dedupe_items_by_url(items, url_key="url")
        # ~6 params/row (site_id, product_url, type, category, first_seen_at, created_at)
        def _call(chunk: list[dict]):
            # per-chunk dedupe guard (prevents CardinalityViolation)
            upsert_product_items(session, site_name, _dedupe_items_by_url(chunk, url_key="url"))
        await _adaptive_upsert(_call, items, approx_cols_per_row=6, start_cap=500, session=session)

    async def _chunked_upsert_urls(session, site_name: str, urls: list[str], ptype: str) -> None:
        if not urls:
            return
        urls = _dedupe_urls(urls)  # global dedupe
        # ~3 params/row (site_id, product_url, created_at)
        def _call(chunk_urls: list[str]):
            upsert_product_urls(session, site_name, _dedupe_urls(chunk_urls), ptype)
        await _adaptive_upsert(_call, urls, approx_cols_per_row=3, start_cap=800, session=session)

    async def _chunked_upsert_details(session, site_name: str, items: list[dict]) -> None:
        if not items:
            return
        items = _dedupe_items_by_url(items, url_key="url")  # global dedupe
        # ~10 params/row (url,title,price,image_url,description,category,amazon_url,first_seen_at,created_at,...)
        def _call(chunk: list[dict]):
            upsert_product_details(session, site_name, _dedupe_items_by_url(chunk, url_key="url"))
        await _adaptive_upsert(_call, items, approx_cols_per_row=10, start_cap=400, session=session)

    # ----------------- params -----------------
    rebaid = {
        "max_pages": int(p.get("rebaid_max_pages", 0)),
        "timeout_ms": int(p.get("rebaid_timeout_ms", 30000)),
        "delay_min": float(p.get("rebaid_delay_min", 0.15)),
        "delay_max": float(p.get("rebaid_delay_max", 0.45)),
    }
    rebatekey_headed: bool = bool(p.get("rebatekey_headed", False))
    myvipon_headed: bool = bool(p.get("myvipon_headed", False))

    rebaid_detail_timeout_ms: int = int(p.get("rebaid_detail_timeout_ms", 12000))
    rebatekey_concurrency: int = int(p.get("rebatekey_concurrency", 12))
    rebatekey_retries: int = int(p.get("rebatekey_retries", 2))
    rebatekey_timeout: float = float(p.get("rebatekey_timeout", 20.0))
    myvipon_workers: int = int(p.get("myvipon_workers", 6))
    myvipon_timeout: int = int(p.get("myvipon_timeout", 30))

    # ----------------- run -----------------
    async def run(job_id: str):
        from db import SessionLocal
        from scrapers.myvipon_details import scrape_details_for_urls as scrape_myvipon_details

        with SessionLocal() as db2:
            # helper: which candidate URLs already exist in DB?
            def _existing_url_set(candidate_urls: list[str]) -> set[str]:
                if not candidate_urls:
                    return set()
                OUT: set[str] = set()
                CH = 1000
                for i in range(0, len(candidate_urls), CH):
                    chunk = candidate_urls[i:i + CH]
                    rows = (
                        db2.query(models.Product.product_url)
                        .filter(models.Product.product_url.in_(chunk))
                        .all()
                    )
                    OUT.update(u for (u,) in rows)
                return OUT

            # ---------- 1) Rebaid URLs ----------
            categories = load_default_rebaid_categories()
            try:
                rebaid_data = await asyncio.to_thread(
                    partial(
                        collect_rebaid_urls,
                        categories=categories,
                        max_pages=rebaid["max_pages"],
                        timeout_ms=rebaid["timeout_ms"],
                        delay_min=rebaid["delay_min"],
                        delay_max=rebaid["delay_max"],
                    )
                )
            except TypeError:
                cfg = {
                    "categories": categories,
                    "max_pages": rebaid["max_pages"],
                    "timeout_ms": rebaid["timeout_ms"],
                    "delay_min": rebaid["delay_min"],
                    "delay_max": rebaid["delay_max"],
                }
                rebaid_data = await asyncio.to_thread(collect_rebaid_urls, cfg)

            def _tag_rebaid(bucket: str):
                items = rebaid_data.get(bucket, []) or []
                return [
                    {
                        "url": it["url"],
                        "price": it.get("price"),
                        "price_value": it.get("price_value"),
                        "category_name": it.get("category_name"),
                        "type": bucket,
                    }
                    for it in items
                ]

            rebaid_items = _tag_rebaid("codes") + _tag_rebaid("cashback") + _tag_rebaid("buyonrebaid")
            rebaid_items = _dedupe_items_by_url(rebaid_items, url_key="url")  # critical
            rebaid_detail_targets = _dedupe_urls([x["url"] for x in rebaid_items])

            rebaid_urls = [x["url"] for x in rebaid_items]
            rebaid_existing = _existing_url_set(rebaid_urls)
            rebaid_new_items = [x for x in rebaid_items if x["url"] not in rebaid_existing]
            if rebaid_new_items:
                await _chunked_upsert_items(db2, "rebaid", rebaid_new_items)
            await job_manager.tick(job_id, ok=True, note="collected rebaid urls", meta={"new": len(rebaid_new_items)})

            # ---------- 2) RebateKey URLs ----------
            rk_data = await asyncio.to_thread(collect_rebatekey_urls, headless=not rebatekey_headed)
            rk_rebate = _dedupe_urls(rk_data.get("rebate_urls", []) or [])
            rk_coupons = _dedupe_urls(rk_data.get("coupons_urls", []) or [])
            rk_detail_targets = _dedupe_urls(rk_rebate + rk_coupons)

            rk_urls_all = rk_rebate + rk_coupons
            rk_existing = _existing_url_set(rk_urls_all)
            rk_rebate_new = [u for u in rk_rebate if u not in rk_existing]
            rk_coupon_new = [u for u in rk_coupons if u not in rk_existing]
            if rk_rebate_new:
                await _chunked_upsert_urls(db2, "rebatekey", rk_rebate_new, "rebate")
            if rk_coupon_new:
                await _chunked_upsert_urls(db2, "rebatekey", rk_coupon_new, "coupon")
            await job_manager.tick(
                job_id,
                ok=True,
                note="collected rebatekey urls",
                meta={"new_rebate": len(rk_rebate_new), "new_coupon": len(rk_coupon_new)},
            )

            # ---------- 3) MyVipon URLs ----------
            mv_data = await asyncio.to_thread(collect_myvipon_urls, headed=myvipon_headed)
            mv_items = []
            for cat_name, urls in (mv_data.get("by_category", {}) or {}).items():
                for u in urls or []:
                    mv_items.append({"url": u, "price_value": None, "type": None, "category_name": cat_name})
            # dedupe across categories & scraper repeats
            mv_items = _dedupe_items_by_url(mv_items, url_key="url")
            mv_detail_targets = _dedupe_urls([x["url"] for x in mv_items])

            mv_urls_all = [x["url"] for x in mv_items]
            mv_existing = _existing_url_set(mv_urls_all)
            mv_new_items = [x for x in mv_items if x["url"] not in mv_existing]
            if mv_new_items:
                await _chunked_upsert_items(db2, "myvipon", mv_new_items)
            await job_manager.tick(job_id, ok=True, note="collected myvipon urls", meta={"new": len(mv_new_items)})

            # ---------- newly inserted URLs (for details) ----------
            new_rebaid_urls = _dedupe_urls([x["url"] for x in rebaid_new_items])
            new_rk_urls = _dedupe_urls(rk_rebate_new + rk_coupon_new)
            new_mv_urls = _dedupe_urls([x["url"] for x in mv_new_items])

            detail_total = len(new_rebaid_urls) + len(new_rk_urls) + len(new_mv_urls)
            if detail_total:
                await job_manager.mark_running(job_id, detail_total)


            # ---------- 4) Rebaid details (visited) ----------
            if rebaid_detail_targets:
                await job_manager.tick(job_id, ok=True, note="collecting rebaid details...")
                ok_items: list[dict] = []
                BATCH = 20
                for i in range(0, len(rebaid_detail_targets), BATCH):
                    chunk = rebaid_detail_targets[i:i + BATCH]
                    data_list = await asyncio.to_thread(
                        scrape_rebaid_details,
                        chunk,
                        timeout_ms=rebaid_detail_timeout_ms
                    )
                    for d in data_list:
                        ok_items.append({
                            "url": d.get("url"),
                            "title": d.get("title"),
                            "description": d.get("description"),
                            "amazon_url": d.get("amazon_url"),
                            "image_url": d.get("image_url"),
                            "price": d.get("price"),
                            "category": d.get("category"),
                        })
                    await job_manager.tick(job_id, ok=True, plus=len(chunk), meta={"site": "rebaid"})
                if ok_items:
                    await _chunked_upsert_details(db2, "rebaid", ok_items)

            # ---------- 5) RebateKey details (visited) ----------
            if rk_detail_targets:
                await job_manager.tick(job_id, ok=True, note="collecting rebatekey details...")
                scraped = await asyncio.to_thread(
                    collect_rebatekey_details,
                    rk_detail_targets,
                    concurrency=rebatekey_concurrency,
                    retries=rebatekey_retries,
                    timeout=rebatekey_timeout,
                )
                items = [{
                    "url": s["url"],
                    "title": s.get("title"),
                    "price": s.get("price"),
                    "image_url": s.get("image_url"),
                    "description": s.get("description"),
                    "category": s.get("category"),
                    "amazon_url": s.get("amazon_url"),
                } for s in scraped]
                if items:
                    await _chunked_upsert_details(db2, "rebatekey", items)
                await job_manager.tick(job_id, ok=True, plus=len(rk_detail_targets), meta={"site": "rebatekey"})

            # ---------- 6) MyVipon details (visited) ----------
            if mv_detail_targets:
                await job_manager.tick(job_id, ok=True, note="collecting myvipon details...")
                ok_items: list[dict] = []
                BATCH = 24
                for i in range(0, len(mv_detail_targets), BATCH):
                    chunk = mv_detail_targets[i:i + BATCH]
                    res = await asyncio.to_thread(
                        scrape_myvipon_details,
                        chunk,
                        workers=myvipon_workers,
                        timeout=myvipon_timeout
                    )
                    for r in res:
                        if r.get("status") == "ok":
                            ok_items.append({
                                "url": r["url"],
                                "title": r.get("title"),
                                "description": r.get("description"),
                                "image_url": r.get("image_url"),
                                "price": r.get("price"),
                                "price_value": r.get("price_value"),
                                "category": r.get("category"),
                                "amazon_url": r.get("amazon_url"),
                            })
                    await job_manager.tick(job_id, ok=True, plus=len(chunk), meta={"site": "myvipon"})
                if ok_items:
                    await _chunked_upsert_details(db2, "myvipon", ok_items)

    return 0, run

@router.post("/cancel/{job_id}")
async def cancel_job(job_id: str):
    st = job_manager.get(job_id)
    if not st:
        raise HTTPException(404, "job not found")

    task = JOB_TASKS.get(job_id)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return {"job_id": job_id, "status": "canceled"}

    # No live task? Mark it canceled anyway to clear the UI.
    await job_manager.finish(job_id, "canceled", note="canceled by request (no live task)")
    return {"job_id": job_id, "status": "canceled"}


@router.post("/cancel-all")
async def cancel_all_jobs(kind: str | None = Query(None, description="Optional filter by kind")):
    canceled = []
    # Cancel live tasks first
    for jid, task in list(JOB_TASKS.items()):
        if kind and (job_manager.get(jid) or {}).get("kind") != kind:
            continue
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            canceled.append(jid)

    # Also sweep any “running” without a live task (stale after crash/restart)
    # If your job_manager exposes a way to iterate, clear those too:
    try:
        for st in job_manager.all():  # if available
            if st.status == "running" and (kind is None or st.kind == kind) and st.id not in JOB_TASKS:
                await job_manager.finish(st.id, "canceled", note="canceled (no live task)")
                canceled.append(st.id)
    except Exception:
        # If job_manager has no .all(), it's fine; we still canceled the live ones.
        pass

    return {"canceled": sorted(set(canceled))}