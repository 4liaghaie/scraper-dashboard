# scheduler.py
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional, List
from zoneinfo import ZoneInfo
from datetime import datetime, timezone

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from settings import settings

log = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

API_BASE: str = getattr(settings, "api_base", None) or "http://127.0.0.1:8000"
_TZ: ZoneInfo = ZoneInfo(getattr(settings, "timezone", "UTC"))
_DAILY_LOCK = asyncio.Lock()

# If you added cancel endpoints, set True to cancel overlaps before running
CANCEL_OVERLAPS = False

# Google export config
GSHEET_ID: Optional[str] = (
    getattr(settings, "google_sheet_id", None) or getattr(settings, "GOOGLE_SHEET_ID", None)
)
# We’ll overwrite the worksheet per-site, so this is not used directly anymore
GSHEET_MODE: str = (
    (getattr(settings, "google_sheet_mode", None) or getattr(settings, "GOOGLE_SHEET_MODE", None) or "replace")
    .lower()
)
if GSHEET_MODE not in {"append", "replace"}:
    GSHEET_MODE = "replace"

# Your export route path. If you mounted it under /exports, change to "/exports/products.google-sheet".
EXPORT_ROUTE = "/exports/products.google-sheet"

# -------------------------------------------------------------------
# HTTP helpers
# -------------------------------------------------------------------

def _client(timeout: float = 60.0) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=API_BASE,
        timeout=timeout,
        headers={"Accept": "application/json"},
    )


async def _kick_job(kind: str, params: Dict[str, Any] | None = None) -> Optional[str]:
    async with _client(timeout=90.0) as client:
        try:
            r = await client.post("/jobs/start", json={"kind": kind, "params": params or {}})
            r.raise_for_status()
            data = r.json()
            job_id = data.get("job_id")
            log.info("[scheduler] started job kind=%s id=%s", kind, job_id)
            return job_id
        except httpx.HTTPStatusError as e:
            log.warning("[scheduler] couldn't start %s (status=%s): %s", kind, e.response.status_code, e.response.text)
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
    stop_status = {"done", "error", "canceled", "cancelled"}
    deadline = asyncio.get_event_loop().time() + timeout

    async with _client(timeout=30.0) as client:
        while True:
            try:
                r = await client.get(f"/jobs/status/{job_id}")
                if r.status_code == 404:
                    await asyncio.sleep(poll_every)
                    continue
                r.raise_for_status()
                st = r.json()
                status = (st.get("status") or "").lower()
                if status in stop_status:
                    log.info("[scheduler] job %s finished with status=%s", job_id, status)
                    return st
            except (httpx.RemoteProtocolError, httpx.ReadError, httpx.ConnectError) as e:
                log.warning("[scheduler] transient error polling job %s: %s", job_id, e)
            except Exception as e:
                log.warning("[scheduler] error polling job %s: %s", job_id, e)

            if asyncio.get_event_loop().time() > deadline:
                raise TimeoutError(f"job {job_id} timed out")

            await asyncio.sleep(poll_every)

# -------------------------------------------------------------------
# Auth helper
# -------------------------------------------------------------------

async def _get_service_token() -> Optional[str]:
    su_email = getattr(settings, "superuser_email", None)
    su_pw = getattr(settings, "superuser_password", None)
    pw = su_pw.get_secret_value() if su_pw else None
    if not (su_email and pw):
        log.warning("[scheduler] SUPERUSER_EMAIL/PASSWORD not set; cannot call protected routes")
        return None

    async with _client(timeout=30.0) as client:
        try:
            data = {"username": su_email, "password": pw}
            r = await client.post("/auth/login", data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
            r.raise_for_status()
            token = r.json().get("access_token")
            if not token:
                log.warning("[scheduler] auth login succeeded but no access_token returned")
                return None
            return token
        except Exception as e:
            log.exception("[scheduler] failed to obtain service token: %s", e)
            return None

# -------------------------------------------------------------------
# Site list + Export helpers
# -------------------------------------------------------------------

async def _list_sites() -> List[str]:
    """
    Uses your metrics endpoint to discover site names.
    Expected payload: [{ "site": "rebaid", "count": 123 }, ...]
    """
    async with _client(timeout=30.0) as client:
        r = await client.get("/metrics/products/by-site")
        r.raise_for_status()
        rows = r.json() or []
        names = [str(x.get("site") or "").strip() for x in rows if x.get("site")]
        # de-dup and keep order
        seen, out = set(), []
        for n in names:
            if n not in seen:
                seen.add(n)
                out.append(n)
        return out


async def _export_site_to_sheet(site_name: str, token: str) -> None:
    """
    Export ALL products for a single site to a worksheet named exactly as the site.
    We ask the API to sort by last_seen_at DESC.
    """
    if not GSHEET_ID:
        log.info("[scheduler] GOOGLE_SHEET_ID not set; skipping export for site=%s", site_name)
        return

    body = {
        "spreadsheet_id": GSHEET_ID,
        "worksheet": site_name,     # sheet name = site name
        "mode": "replace",          # replace the whole sheet each day
        # "start_cell": "A1",       # optional (default in your route is A1)
    }
    params = {
        "site": site_name,
        "sort": "last_seen_desc",   # tiny route tweak below will honor this
        # "limit": 200000,          # optional: omit to export all
    }
    hdrs = {"Authorization": f"Bearer {token}"}

    async with _client(timeout=600.0) as client:  # large sheets need generous timeout
        try:
            r = await client.post(EXPORT_ROUTE, json=body, params=params, headers=hdrs)
            r.raise_for_status()
            res = r.json()
            log.info(
                "[scheduler] Export OK site=%s rows=%s range=%s sheet=%s",
                site_name, res.get("written_rows"), res.get("updated_range"), res.get("worksheet")
            )
        except httpx.HTTPStatusError as e:
            log.error(
                "[scheduler] Export FAILED site=%s status=%s: %s",
                site_name, e.response.status_code, e.response.text
            )
        except Exception as e:
            log.exception("[scheduler] Export error site=%s: %s", site_name, e)


async def _export_all_sites_to_sheets() -> None:
    token = await _get_service_token()
    if not token:
        log.warning("[scheduler] no service token; skipping per-site exports")
        return

    sites = await _list_sites()
    if not sites:
        log.info("[scheduler] no sites discovered; skipping export")
        return

    # Run exports sequentially to avoid Sheets quota bursts. (You can parallelize if needed.)
    for name in sites:
        await _export_site_to_sheet(name, token)

# -------------------------------------------------------------------
# Pipelines
# -------------------------------------------------------------------

async def run_daily_pipeline():
    """
    Runs once per day:
      1) full_fresh_run (URLs + details for new URLs; no store step)
      2) amazon_stores (missing_only)
      3) export ALL products per-site -> separate worksheet per site, sorted by last_seen_at desc
    """
    async with _DAILY_LOCK:
        log.info("[scheduler] starting daily pipeline")

        if CANCEL_OVERLAPS:
            try:
                async with _client(timeout=30.0) as client:
                    await client.post("/jobs/cancel-all", params={"kind": "full_fresh_run"})
                    await client.post("/jobs/cancel-all", params={"kind": "amazon_stores"})
            except Exception:
                log.warning("[scheduler] cancel-all preflight failed (continuing)")

        # --- 1) full_fresh_run ---
        ff_params = {
            "rebaid_max_pages": 0,
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
            await _wait_for_job(ff_job, poll_every=3.0, timeout=3 * 3600)
        else:
            log.warning("[scheduler] full_fresh_run not started; skipping wait")

        # --- 2) amazon_stores ---
        st_params = {"missing_only": True, "limit": 6000, "timeout_ms": 12000}
        st_job = await _kick_job("amazon_stores", st_params)
        if st_job:
            await _wait_for_job(st_job, poll_every=3.0, timeout=2 * 3600)
        else:
            log.warning("[scheduler] amazon_stores not started; skipping wait")

        # --- 3) per-site full export -> Google Sheets ---
        await _export_all_sites_to_sheets()

        log.info("[scheduler] daily pipeline finished")

# -------------------------------------------------------------------
# Scheduler wiring
# -------------------------------------------------------------------

def build_scheduler() -> AsyncIOScheduler:
    sched = AsyncIOScheduler(timezone=_TZ)
    sched.add_job(
        run_daily_pipeline,
        CronTrigger(hour=10, minute=29, timezone=_TZ),
        id="daily_full_fresh_plus_stores",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=3600,
        max_instances=1,
    )
    return sched


def start_scheduler_in_app(app) -> AsyncIOScheduler:
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
