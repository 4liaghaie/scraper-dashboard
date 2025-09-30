from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.orm import Session
from db import get_session
from routers.jobs import router as jobs_router
from routers.routers_sites import router as sites_router
from routers.routers_scrape import router as scrape_router
from routers.routers_products import router as products_router
from routers.routers_auth import router as auth_router
from routers.routers_admin_users import router as admin_users_router
from routers.routers_profile import router as profile_router
from routers.product_actions import router as product_actions_router
from routers.metrics import router as metrics_router
from fastapi.middleware.cors import CORSMiddleware

from jobs.manager import JobManager
from security import hash_password
from settings import settings
from db import SessionLocal
import models
from scheduler import build_scheduler, run_daily_pipeline
from fastapi import BackgroundTasks
import asyncio

app = FastAPI(title="Scraper API")
scheduler = build_scheduler()
resolved_origins = settings.cors_origins or ["http://localhost:3000"]
allow_credentials = False if resolved_origins == ["*"] else True

app.add_middleware(
    CORSMiddleware,
    allow_origins=resolved_origins,
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
    # expose_headers=["X-Total-Count"]  # if you truly need expose, list explicit headers
)

@app.on_event("startup")
def ensure_superuser():
    with SessionLocal() as db:
        # if there are no users at all, create the bootstrap superuser (if envs provided)
        if db.query(models.User).count() == 0:
            if settings.superuser_email and settings.superuser_password:
                u = models.User(
                    email=str(settings.superuser_email),
                    hashed_password=hash_password(settings.superuser_password.get_secret_value()),
                    role="superuser",
                    is_active=True,
                )
                db.add(u)
                db.commit()
                print(f"[bootstrap] Superuser created: {u.email}")
            else:
                print("[bootstrap] No users exist and SUPERUSER_* not set. Set envs to create the first superuser.")
    # start scheduler
    if not scheduler.running:
        scheduler.start()
        print("[scheduler] started")
        # after scheduler.start() in main.py
        for j in scheduler.get_jobs():
            print(f"[scheduler] loaded {j.id} next_run_time={j.next_run_time}")

@app.on_event("shutdown")
def on_shutdown():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        print("[scheduler] stopped")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://89.116.157.224:3000",   

    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(profile_router)

app.include_router(sites_router)
app.include_router(scrape_router)
app.include_router(products_router)
app.include_router(auth_router)
app.include_router(admin_users_router)
app.include_router(product_actions_router)
app.include_router(metrics_router)


@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/db/health")
def db_health(db: Session = Depends(get_session)):
    # simple round-trip
    db.execute(text("SELECT 1"))
    return {"db": "ok"}

app.include_router(jobs_router)

@app.get("/_debug/scheduler/jobs")
def _debug_scheduler_jobs():
    return [
        {
            "id": j.id,
            "next_run_time": j.next_run_time.isoformat() if j.next_run_time else None,
        }
        for j in scheduler.get_jobs()
    ]

@app.post("/_debug/scheduler/run-now")
async def _debug_scheduler_run_now(background: BackgroundTasks):
    # do not block the request; run in background
    background.add_task(run_daily_pipeline)
    return {"queued": True}