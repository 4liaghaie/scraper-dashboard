# scheduler.py
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional
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

# Where your FastAPI is listening (internal URL the scheduler should call)
API_BASE: str = getattr(settings, "api_base", None) or "http://127.0.0.1:8000"

# Optional: use a timezone from settings; fallback to UTC
_TZ: ZoneInfo = ZoneInfo(getattr(settings, "timezone", "UTC"))

# Prevent overlapping daily pipeline runs
_DAILY_LOCK = asyncio.Lock()

# If you added cancel endpoints, set True to cancel overlaps before running
CANCEL_OVERLAPS = False  # keep False if you didn’t add /jobs/cancel-all

# Google export config from env (add these to your .env / container)
GSHEET_ID: Optional[str] = getattr(settings, "google_sheet_id", None) or getattr(settings, "GOOGLE_SHEET_ID", None)
GSHEET_WORKSHEET: str = getattr(settings, "google_sheet_worksheet", None) or getattr(settings, "GOOGLE_SHEET_WORKSHEET", None) or "Daily"
GSHEET_MODE: str = (getattr(settings, "google_sheet_mode", None) or getattr(settings, "GOOGLE_SHEET_MODE", None) or "append").lower()
if GSHEET_MODE not in {"append", "replace"}:
    GSHEET_MODE = "append"

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
# Auth helper (uses your superuser credentials to call protected routes)
# -------------------------------------------------------------------

async def _get_service_token() -> Optional[str]:
    """
    Log in with SUPERUSER_EMAIL / SUPERUSER_PASSWORD to obtain a JWT for internal calls.
    """
    su_email = getattr(settings, "superuser_email", None)
    su_pw = getattr(settings, "superuser_password", None)
    pw = su_pw.get_secret_value() if su_pw else None
    if not (su_email and pw):
        log.warning("[scheduler] SUPERUSER_EMAIL/PASSWORD not set; cannot call protected routes")
        return None

    async with _client(timeout=30.0) as client:
        try:
            data = {"username": su_email, "password": pw}
            # Your /auth/login expects x-www-form-urlencoded
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
# Export-to-Google-Sheets
# -------------------------------------------------------------------

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


async def _export_recent_products_to_sheet(window_start: datetime, window_end: datetime) -> None:
    """
    Call your existing /products.google-sheet route with last_seen_from/to = run window.
    Respects GSHEET_* settings above. Logs errors but doesn't crash the scheduler.
    """
    if not GSHEET_ID:
        log.info("[scheduler] GOOGLE_SHEET_ID not set; skipping Google Sheets export")
        return

    token = await _get_service_token()
    if not token:
        log.warning("[scheduler] no service token; skipping Google Sheets export")
        return

    body = {
        "spreadsheet_id": GSHEET_ID,
        "worksheet": GSHEET_WORKSHEET,
        "mode": GSHEET_MODE,          # "append" or "replace"
        # "start_cell": "A1",         # only needed when mode == "replace"
    }
    params = {
        "last_seen_from": _iso(window_start),
        "last_seen_to": _iso(window_end),
        # You could also pass site / ids / limit, but the time window is the key.
    }

    hdrs = {"Authorization": f"Bearer {token}"}

    async with _client(timeout=300.0) as client:  # longer timeout for big writes
        try:
            r = await client.post("exports/products.google-sheet", json=body, params=params, headers=hdrs)
            r.raise_for_status()
            res = r.json()
            written = res.get("written_rows")
            updated_range = res.get("updated_range")
            log.info(
                "[scheduler] Google Sheets export OK: rows=%s range=%s worksheet=%s",
                written, updated_range, GSHEET_WORKSHEET
            )
        except httpx.HTTPStatusError as e:
            log.error(
                "[scheduler] Google Sheets export failed (%s): %s",
                e.response.status_code, e.response.text
            )
        except Exception as e:
            log.exception("[scheduler] Google Sheets export error: %s", e)

# -------------------------------------------------------------------
# Pipelines
# -------------------------------------------------------------------

async def run_daily_pipeline():
    """
    Runs once per day:
      1) full_fresh_run (URLs + details for new URLs; no store step)
      2) amazon_stores (missing_only)
      3) export everything seen in this window to Google Sheet
    """
    async with _DAILY_LOCK:
        window_start = datetime.now(timezone.utc)
        log.info("[scheduler] starting daily pipeline (window_start=%s)", window_start.isoformat())

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

        window_end = datetime.now(timezone.utc)
        log.info("[scheduler] daily pipeline finished (window_end=%s)", window_end.isoformat())

        # --- 3) Export everything seen in this run window ---
        await _export_recent_products_to_sheet(window_start, window_end)

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
        CronTrigger(hour=22, minute=00, timezone=_TZ),
        id="daily_full_fresh_plus_stores",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=3600,
        max_instances=1,
    )
    return sched


def start_scheduler_in_app(app) -> AsyncIOScheduler:
    """
    Attach scheduler to FastAPI app lifespan.
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
