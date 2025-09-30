from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from passlib.context import CryptContext
import jwt
from pydantic import SecretStr  # <-- for isinstance check
from db import get_session
import models
from settings import settings

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

def hash_password(pw: str) -> str:
    return pwd_ctx.hash(pw)

def verify_password(pw: str, hashed: str) -> bool:
    return pwd_ctx.verify(pw, hashed)

def _jwt_secret_value() -> str:
    s = settings.jwt_secret
    # Works whether jwt_secret is a str or a SecretStr
    if isinstance(s, SecretStr):
        return s.get_secret_value()
    return str(s)

def create_access_token(sub: str, minutes: int | None = None) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=minutes or settings.access_token_minutes)
    payload = {"sub": sub, "exp": exp}
    return jwt.encode(payload, _jwt_secret_value(), algorithm=settings.jwt_algorithm)

def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, _jwt_secret_value(), algorithms=[settings.jwt_algorithm])
    except jwt.PyJWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

def get_current_user(db: Session = Depends(get_session), token: str = Depends(oauth2_scheme)) -> models.User:
    payload = decode_token(token)
    email: Optional[str] = payload.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    user = db.query(models.User).filter(models.User.email == email).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="Inactive or missing user")
    return user
def require_role(*roles: str):
    def dep(user: models.User = Depends(get_current_user)) -> models.User:
        if user.role not in roles:
            raise HTTPException(status_code=403, detail="Forbidden")
        return user
    return dep
