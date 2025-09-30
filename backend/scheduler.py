# scheduler.py
from __future__ import annotations
import asyncio
from typing import Any, Dict

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from settings import settings

# Where your FastAPI is listening (same base your frontend uses works fine)
API_BASE = getattr(settings, "api_base", None) or "http://127.0.0.1:8000"

# Prevent overlapping daily pipeline runs
_DAILY_LOCK = asyncio.Lock()


async def _kick_job(kind: str, params: Dict[str, Any] | None = None) -> str:
    """
    Start a job via /jobs/start and return the job_id.
    """
    async with httpx.AsyncClient(base_url=API_BASE, timeout=60.0) as client:
        r = await client.post("/jobs/start", json={"kind": kind, "params": params or {}})
        r.raise_for_status()
        data = r.json()
        return data["job_id"]


async def _wait_for_job(job_id: str, *, poll_every: float = 2.0, timeout: float = 3600.0) -> Dict[str, Any]:
    """
    Poll /jobs/status/{id} until the job is done/error/cancelled (or timeout).
    Returns the final job state dict.
    """
    stop_status = {"done", "error", "cancelled"}
    deadline = asyncio.get_event_loop().time() + timeout

    async with httpx.AsyncClient(base_url=API_BASE, timeout=30.0) as client:
        while True:
            r = await client.get(f"/jobs/status/{job_id}")
            if r.status_code == 404:
                # brief grace for job manager create/start window
                await asyncio.sleep(poll_every)
                continue
            r.raise_for_status()
            st = r.json()
            if st.get("status") in stop_status:
                return st
            if asyncio.get_event_loop().time() > deadline:
                raise TimeoutError(f"job {job_id} timed out")
            await asyncio.sleep(poll_every)


async def run_daily_pipeline():
    """
    Runs once per day:
      1) full_fresh_run (URLs + details for new URLs; no store step)
      2) amazon_stores (missing_only)
    Sequential, with waiting between steps.
    """
    async with _DAILY_LOCK:
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
        await _wait_for_job(ff_job, poll_every=3.0, timeout=3 * 3600)  # up to 3h

        # --- 2) amazon_stores (missing only) ---
        stores_params = {
            "missing_only": True,
            "limit": 6000,          # raise/lower depending on volume and rate limits
            "timeout_ms": 12000,
        }
        st_job = await _kick_job("amazon_stores", stores_params)
        await _wait_for_job(st_job, poll_every=3.0, timeout=2 * 3600)


def build_scheduler() -> AsyncIOScheduler:
    """
    Build and return a scheduler with a single daily job.
    """
    sched = AsyncIOScheduler()  # default timezone = UTC in APScheduler 3
    # Run once daily at 03:00 (change hour/minute to your preferred wall time)
    sched.add_job(
        run_daily_pipeline,
        CronTrigger(hour=22, minute=46),
        id="daily_full_fresh_plus_stores",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=3600,  # if missed by < 1h, run once on resume
        max_instances=1,
    )
    return sched
