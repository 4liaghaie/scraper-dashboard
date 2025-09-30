from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from db import get_session
import models
from schemas import JobCreate, JobOut

router = APIRouter(prefix="/jobs", tags=["jobs"])

@router.get("", response_model=List[JobOut])
def list_jobs(db: Session = Depends(get_session)):
    return db.query(models.Job).order_by(models.Job.id.desc()).all()

@router.post("", response_model=JobOut, status_code=201)
def create_job(payload: JobCreate, db: Session = Depends(get_session)):
    job = models.Job(site=payload.site, name=payload.name, schedule_cron=payload.schedule_cron)
    db.add(job)
    db.commit()
    db.refresh(job)
    return job

@router.get("/{job_id}", response_model=JobOut)
def get_job(job_id: int, db: Session = Depends(get_session)):
    job = db.query(models.Job).get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job
