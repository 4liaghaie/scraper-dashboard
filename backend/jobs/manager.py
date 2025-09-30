# jobs/manager.py
from __future__ import annotations
import asyncio, time, uuid
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

@dataclass
class JobState:
    id: str
    kind: str
    status: str         # "queued" | "running" | "done" | "error" | "cancelled"
    started_at: float
    finished_at: Optional[float]
    total: int
    done: int
    ok: int
    err: int
    note: str
    meta: Dict[str, Any]

class JobManager:
    def __init__(self):
        self.jobs: Dict[str, JobState] = {}
        self.queues: Dict[str, asyncio.Queue] = {}
        self.cancels: Dict[str, asyncio.Event] = {}
        self.lock = asyncio.Lock()

    def _new_id(self): return uuid.uuid4().hex

    async def create(self, kind: str, total: int = 0, meta: Optional[dict] = None) -> JobState:
        async with self.lock:
            jid = self._new_id()
            st = JobState(
                id=jid, kind=kind, status="queued",
                started_at=time.time(), finished_at=None,
                total=total, done=0, ok=0, err=0, note="", meta=meta or {}
            )
            self.jobs[jid] = st
            self.queues[jid] = asyncio.Queue()
            self.cancels[jid] = asyncio.Event()
            return st

    def get(self, job_id: str) -> Optional[JobState]:
        return self.jobs.get(job_id)

    def cancel_event(self, job_id: str) -> Optional[asyncio.Event]:
        return self.cancels.get(job_id)

    async def push(self, job_id: str, event: dict):
        q = self.queues.get(job_id)
        if q:
            await q.put(event)

    async def stream(self, job_id: str):
        q = self.queues.get(job_id)
        if not q: return
        # drain until DONE/ERROR + queue empty
        while True:
            event = await q.get()
            yield event
            if event.get("type") in ("done", "error", "cancelled") and q.empty():
                break

    async def mark_running(self, job_id: str, total: Optional[int] = None):
        st = self.jobs[job_id]
        st.status = "running"
        if total is not None: st.total = total
        await self.push(job_id, {"type": "started", "state": asdict(st)})

    async def tick(self, job_id: str, *, ok: bool, note: str = "", plus: int = 1, meta: Optional[dict]=None):
        st = self.jobs[job_id]
        st.done += plus
        if ok: st.ok += plus
        else: st.err += plus
        if note: st.note = note
        if meta: st.meta.update(meta)
        await self.push(job_id, {"type": "progress", "state": asdict(st)})

    async def finish(self, job_id: str, status: str = "done", note: str = ""):
        st = self.jobs[job_id]
        st.status = status
        st.finished_at = time.time()
        if note: st.note = note
        await self.push(job_id, {"type": status, "state": asdict(st)})
        # close queue
        await self.push(job_id, {"type": "end"})
