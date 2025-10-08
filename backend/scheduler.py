# scheduler.py
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from settings import settings

log = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

# Where your FastAPI is listening (internal URL the scheduler should call)
API_BASE: str = getattr(settings, "api_base", None) or "http://127.0.0.1:8000"

# Optional: use a timezone from settings; fallback to UTC
_TZ: ZoneInfo = ZoneInfo(getattr(settings, "timezone", "UTC"))

# Prevent overlapping daily pipeline runs
_DAILY_LOCK = asyncio.Lock()

# If you added the cancel endpoints I suggested, set this True to
# cancel stale/overlapping runs before kicking a new one.
CANCEL_OVERLAPS = False  # keep False if you didn’t add /jobs/cancel-all


# -------------------------------------------------------------------
# HTTP helpers
# -------------------------------------------------------------------

def _client(timeout: float = 60.0) -> httpx.AsyncClient:
    # A single place to tweak HTTP client config
    return httpx.AsyncClient(
        base_url=API_BASE,
        timeout=timeout,
        headers={"Accept": "application/json"},
    )


async def _kick_job(kind: str, params: Dict[str, Any] | None = None) -> Optional[str]:
    """
    Start a job via /jobs/start and return the job_id (or None if refused/overlapping).
    """
    async with _client(timeout=90.0) as client:
        try:
            r = await client.post("/jobs/start", json={"kind": kind, "params": params or {}})
            r.raise_for_status()
            data = r.json()
            job_id = data.get("job_id")
            log.info("[scheduler] started job kind=%s id=%s", kind, job_id)
            return job_id
        except httpx.HTTPStatusError as e:
            # If you added any_running guards that return 409, handle it nicely
            status = e.response.status_code
            text = e.response.text
            log.warning("[scheduler] couldn't start %s (status=%s): %s", kind, status, text)
            return None
        except Exception as e:
            log.exception("[scheduler] error starting job %s: %s", kind, e)
            return None


async def _wait_for_job(
    job_id: str,
    *,
    poll_every: float = 2.0,
    timeout: float = 3600.0,
) -> Dict[str, Any]:
    """
    Poll /jobs/status/{id} until the job is done/error/canceled (or timeout).
    Returns the final job state dict.
    """
    stop_status = {"done", "error", "canceled", "cancelled"}  # be tolerant of both spellings
    deadline = asyncio.get_event_loop().time() + timeout

    async with _client(timeout=30.0) as client:
        while True:
            try:
                r = await client.get(f"/jobs/status/{job_id}")
                if r.status_code == 404:
                    # brief grace for job manager create/start window
                    await asyncio.sleep(poll_every)
                    continue
                r.raise_for_status()
                st = r.json()
                status = (st.get("status") or "").lower()
                if status in stop_status:
                    log.info("[scheduler] job %s finished with status=%s", job_id, status)
                    return st
            except (httpx.RemoteProtocolError, httpx.ReadError, httpx.ConnectError) as e:
                # Transient network hiccups — just retry
                log.warning("[scheduler] transient error polling job %s: %s", job_id, e)
            except Exception as e:
                # Don’t crash the app because polling glitched; keep trying until timeout
                log.warning("[scheduler] error polling job %s: %s", job_id, e)

            if asyncio.get_event_loop().time() > deadline:
                raise TimeoutError(f"job {job_id} timed out")

            await asyncio.sleep(poll_every)


# -------------------------------------------------------------------
# Pipelines
# -------------------------------------------------------------------

async def run_daily_pipeline():
    """
    Runs once per day:
      1) full_fresh_run (URLs + details for new URLs; no store step)
      2) amazon_stores (missing_only)
    Sequential, with waiting between steps, and a lock to avoid overlap.
    """
    async with _DAILY_LOCK:
        log.info("[scheduler] starting daily pipeline")

        # (Optional) sweep overlaps if you added cancel endpoints
        if CANCEL_OVERLAPS:
            try:
                async with _client(timeout=30.0) as client:
                    await client.post("/jobs/cancel-all", params={"kind": "full_fresh_run"})
                    await client.post("/jobs/cancel-all", params={"kind": "amazon_stores"})
            except Exception:
                log.warning("[scheduler] cancel-all preflight failed (continuing)")

        # --- 1) full_fresh_run ---
        ff_params = {
            # tune as you like; these are safe defaults
            "rebaid_max_pages": 0,          # 0 = all
            "rebaid_timeout_ms": 30000,
            "rebatekey_headed": False,
            "myvipon_headed": True,
            "rebaid_detail_timeout_ms": 12000,
            "rebatekey_concurrency": 12,
            "rebatekey_retries": 2,
            "rebatekey_timeout": 20.0,
            "myvipon_workers": 8,
            "myvipon_timeout": 30,
        }
        ff_job = await _kick_job("full_fresh_run", ff_params)
        if ff_job:
            await _wait_for_job(ff_job, poll_every=3.0, timeout=3 * 3600)  # up to 3h
        else:
            log.warning("[scheduler] full_fresh_run was not started; skipping wait")

        # --- 2) amazon_stores (missing only) ---
        stores_params = {
            "missing_only": True,
            "limit": 6000,          # adjust to your volume/rate limits
            "timeout_ms": 12000,
        }
        st_job = await _kick_job("amazon_stores", stores_params)
        if st_job:
            await _wait_for_job(st_job, poll_every=3.0, timeout=2 * 3600)
        else:
            log.warning("[scheduler] amazon_stores was not started; skipping wait")

        log.info("[scheduler] daily pipeline finished")


# -------------------------------------------------------------------
# Building / starting the scheduler
# -------------------------------------------------------------------

def build_scheduler() -> AsyncIOScheduler:
    """
    Build and return a scheduler with a single daily job.
    """
    sched = AsyncIOScheduler(timezone=_TZ)
    # Example: 22:00 every day in the configured timezone
    sched.add_job(
        run_daily_pipeline,
        CronTrigger(hour=22, minute=0, timezone=_TZ),
        id="daily_full_fresh_plus_stores",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=3600,  # if missed by < 1h, run once on resume
        max_instances=1,
    )
    return sched


def start_scheduler_in_app(app) -> AsyncIOScheduler:
    """
    Helper to attach to FastAPI app lifespan:
        from scheduler import start_scheduler_in_app
        sched = start_scheduler_in_app(app)
    """
    sched = build_scheduler()

    @app.on_event("startup")
    async def _start():
        sched.start()
        next_run = sched.get_job("daily_full_fresh_plus_stores").next_run_time
        log.info("[scheduler] loaded daily_full_fresh_plus_stores next_run_time=%s", next_run)

    @app.on_event("shutdown")
    async def _stop():
        sched.shutdown(wait=False)

    return sched
