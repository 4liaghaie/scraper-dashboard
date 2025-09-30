# services/job_manager.py

from __future__ import annotations
from typing import Any, Dict, Optional
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

import models
from db import SessionLocal


class JobManager:
    def __init__(self, SessionFactory=SessionLocal):
        self.SessionFactory = SessionFactory

    # --------- Job + Run lifecycle ---------
    def start_run(
        self,
        job_name: str,
        *,
        schedule_cron: str = "",
        is_active: bool = True,
        total: int = 0,
        note: str = "",
        meta: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Create (or reuse) Job row and queue a new JobRun. Returns run_id."""
        with self.SessionFactory() as db:
            job = self._get_or_create_job(db, job_name, schedule_cron, is_active)
            run = models.JobRun(
                job_id=job.id,
                status="queued",
                queued_at=datetime.utcnow(),
                total=total or 0,
                processed=0,
                ok_count=0,
                fail_count=0,
                note=note or "",
                meta=meta or {},
            )
            db.add(run)
            db.commit()
            db.refresh(run)
            return run.id

    def mark_running(self, run_id: int, total: Optional[int] = None, note: str = "") -> None:
        """Flip a queued run to running (or adjust running), optionally setting total."""
        with self.SessionFactory() as db:
            run = db.get(models.JobRun, run_id)
            if not run:
                return
            run.status = "running"
            run.started_at = run.started_at or datetime.utcnow()
            if total is not None:
                run.total = int(total)
            if note:
                run.note = note
            db.commit()

    def finish_ok(self, run_id: int, *, note: str = "", meta: Optional[Dict[str, Any]] = None) -> None:
        with self.SessionFactory() as db:
            run = db.get(models.JobRun, run_id)
            if not run:
                return
            run.status = "done"
            run.finished_at = datetime.utcnow()
            if note:
                run.note = note
            if meta:
                run.meta = {**(run.meta or {}), **meta}
            db.commit()

    def finish_error(self, run_id: int, *, error_text: str, note: str = "", meta: Optional[Dict[str, Any]] = None) -> None:
        with self.SessionFactory() as db:
            run = db.get(models.JobRun, run_id)
            if not run:
                return
            run.status = "error"
            run.finished_at = datetime.utcnow()
            run.error_text = (run.error_text or "") + (("\n" if run.error_text else "") + (error_text or ""))
            if note:
                run.note = note
            if meta:
                run.meta = {**(run.meta or {}), **meta}
            db.commit()

    def cancel(self, run_id: int, *, note: str = "") -> None:
        with self.SessionFactory() as db:
            run = db.get(models.JobRun, run_id)
            if not run:
                return
            run.status = "canceled"
            run.finished_at = datetime.utcnow()
            if note:
                run.note = note
            db.commit()

    # --------- Progress & events ---------
    def tick(
        self,
        run_id: int,
        *,
        plus: int = 0,
        ok: bool = False,
        fail: int = 0,
        level: str = "info",
        note: str = "",
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Increment counters and append a JobEvent.
        - plus increments processed
        - ok=True increments ok_count by plus (or by 1 if plus==0)
        - fail increments fail_count by `fail` (defaults to 0)
        """
        with self.SessionFactory() as db:
            run = db.get(models.JobRun, run_id)
            if not run:
                return

            inc_processed = int(plus or 0)
            inc_ok = inc_processed if ok else 0
            if ok and inc_ok == 0:
                inc_ok = 1  # allow ok without plus

            run.processed = (run.processed or 0) + inc_processed
            run.ok_count = (run.ok_count or 0) + inc_ok
            run.fail_count = (run.fail_count or 0) + int(fail or 0)
            if note:
                run.note = note

            ev = models.JobEvent(
                run_id=run.id,
                ts=datetime.utcnow(),
                level=(level or "info")[:10],
                message=(note or "")[:400],
                plus=inc_processed,
                meta=meta or {},
            )
            db.add(ev)
            db.commit()

    # --------- Per-site / per-stage parts (optional) ---------
    def get_or_create_part(self, run_id: int, site: str, stage: str = "urls") -> int:
        with self.SessionFactory() as db:
            part = (
                db.query(models.JobRunPart)
                .filter(models.JobRunPart.run_id == run_id,
                        models.JobRunPart.site == site,
                        models.JobRunPart.stage == stage)
                .first()
            )
            if not part:
                part = models.JobRunPart(
                    run_id=run_id,
                    site=site,
                    stage=stage,
                    status="queued",
                    total=0,
                    processed=0,
                    ok_count=0,
                    fail_count=0,
                    note="",
                    meta={},
                )
                db.add(part)
                db.commit()
                db.refresh(part)
            return part.id

    def mark_part_running(self, part_id: int, total: Optional[int] = None, note: str = "") -> None:
        with self.SessionFactory() as db:
            part = db.get(models.JobRunPart, part_id)
            if not part:
                return
            part.status = "running"
            if total is not None:
                part.total = int(total)
            if note:
                part.note = note
            db.commit()

    def tick_part(
        self,
        part_id: int,
        *,
        plus: int = 0,
        ok: bool = False,
        fail: int = 0,
        note: str = "",
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.SessionFactory() as db:
            part = db.get(models.JobRunPart, part_id)
            if not part:
                return
            inc_processed = int(plus or 0)
            inc_ok = inc_processed if ok else 0
            if ok and inc_ok == 0:
                inc_ok = 1

            part.processed = (part.processed or 0) + inc_processed
            part.ok_count = (part.ok_count or 0) + inc_ok
            part.fail_count = (part.fail_count or 0) + int(fail or 0)
            if note:
                part.note = note
            if meta:
                part.meta = {**(part.meta or {}), **meta}
            db.commit()

    def finish_part(self, part_id: int, *, status: str = "done", note: str = "", meta: Optional[Dict[str, Any]] = None,
                    error_text: Optional[str] = None) -> None:
        with self.SessionFactory() as db:
            part = db.get(models.JobRunPart, part_id)
            if not part:
                return
            part.status = status
            if note:
                part.note = note
            if meta:
                part.meta = {**(part.meta or {}), **meta}
            if error_text:
                part.error_text = (part.error_text or "") + (("\n" if part.error_text else "") + error_text)
            db.commit()

    # --------- internals ---------
    def _get_or_create_job(self, db: Session, name: str, schedule_cron: str, is_active: bool) -> models.Job:
        job = db.query(models.Job).filter_by(name=name).first()
        if not job:
            job = models.Job(name=name, schedule_cron=schedule_cron or "", is_active=is_active)
            db.add(job)
            db.commit()
            db.refresh(job)
        return job


# convenient singleton
job_manager = JobManager()
