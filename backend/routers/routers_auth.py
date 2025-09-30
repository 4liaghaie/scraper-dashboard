from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from db import get_session
import models
from schemas import  UserOut, TokenOut
from security import  verify_password, create_access_token, get_current_user

router = APIRouter(prefix="/auth", tags=["auth"])



@router.post("/login", response_model=TokenOut)
def login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_session)):
    user = db.query(models.User).filter(models.User.email == form.username).first()
    if not user or not verify_password(form.password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect credentials")
    token = create_access_token(sub=user.email)
    return TokenOut(access_token=token)

@router.get("/me", response_model=UserOut)
def me(user = Depends(get_current_user)):
    return user
