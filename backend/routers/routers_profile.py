from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from db import get_session
import models
from schemas import UserOut, ProfileUpdate, PasswordChangeIn, ProfileResponse
from security import get_current_user, verify_password, hash_password, create_access_token

router = APIRouter(prefix="/profile", tags=["profile"])

@router.get("", response_model=UserOut)
def get_profile(user: models.User = Depends(get_current_user)):
    return user

@router.patch("", response_model=ProfileResponse)
def update_profile(
    payload: ProfileUpdate,
    db: Session = Depends(get_session),
    user: models.User = Depends(get_current_user),
):
    changed = False

    # Update email (unique)
    if payload.email and payload.email != user.email:
        exists = db.query(models.User).filter(models.User.email == payload.email).first()
        if exists:
            raise HTTPException(status_code=400, detail="Email already in use")
        user.email = str(payload.email)
        changed = True

    if not changed:
        return {"user": user}  # nothing changed → no token rotation

    db.commit()
    db.refresh(user)

    # Rotate JWT so "sub" matches new email
    token = create_access_token(sub=user.email)
    return {"user": user, "access_token": token, "token_type": "bearer"}

@router.post("/password", response_model=ProfileResponse)
def change_password(
    payload: PasswordChangeIn,
    db: Session = Depends(get_session),
    user: models.User = Depends(get_current_user),
):
    if not verify_password(payload.current_password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Current password is incorrect")

    if payload.new_password_confirm is not None and payload.new_password != payload.new_password_confirm:
        raise HTTPException(status_code=400, detail="New password confirmation does not match")

    if len(payload.new_password) < 8:
        raise HTTPException(status_code=400, detail="New password must be at least 8 characters")

    user.hashed_password = hash_password(payload.new_password)
    db.commit()
    db.refresh(user)

    # Optional: rotate JWT after password change
    token = create_access_token(sub=user.email)
    return {"user": user, "access_token": token, "token_type": "bearer"}
