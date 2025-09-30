from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from db import get_session
import models
from security import require_role, hash_password
from schemas import UserCreateAdmin, UserUpdateAdmin, UserRow, UserPage

router = APIRouter(
    prefix="/admin/users",
    tags=["users"],
    dependencies=[Depends(require_role("superuser"))],  # superuser-only
)

@router.get("", response_model=UserPage)
def list_users(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_session),
):
    q = db.query(models.User)
    total = q.count()
    items = (
        q.order_by(models.User.id.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return {"items": items, "total": total, "page": page, "page_size": page_size}

@router.post("", response_model=UserRow, status_code=201)
def create_user(payload: UserCreateAdmin, db: Session = Depends(get_session)):
    if db.query(models.User).filter(models.User.email == payload.email).first():
        raise HTTPException(status_code=400, detail="Email already exists")
    u = models.User(
        email=payload.email,
        hashed_password=hash_password(payload.password),
        role=payload.role,
        is_active=True,
    )
    db.add(u)
    db.commit()
    db.refresh(u)
    return u

@router.patch("/{user_id}", response_model=UserRow)
def update_user(user_id: int, payload: UserUpdateAdmin, db: Session = Depends(get_session)):
    u = db.query(models.User).get(user_id)
    if not u:
        raise HTTPException(404, "User not found")
    if payload.password:
        u.hashed_password = hash_password(payload.password)
    if payload.role:
        u.role = payload.role
    if payload.is_active is not None:
        u.is_active = payload.is_active
    db.commit()
    db.refresh(u)
    return u

@router.delete("/{user_id}", status_code=204)
def delete_user(user_id: int, db: Session = Depends(get_session)):
    u = db.query(models.User).get(user_id)
    if not u:
        raise HTTPException(404, "User not found")
    db.delete(u)
    db.commit()
