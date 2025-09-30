from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List
from db import get_session
import models

router = APIRouter(prefix="/sites", tags=["sites"])

@router.get("", response_model=List[dict])
def list_sites(db: Session = Depends(get_session)):
    rows = db.query(models.Site).order_by(models.Site.id).all()
    return [{"id": s.id, "name": s.name, "base_url": s.base_url} for s in rows]
