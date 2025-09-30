from pydantic import BaseModel, Field, EmailStr, HttpUrl
from datetime import datetime
from typing import Optional, List
from decimal import Decimal
   
class JobCreate(BaseModel):
    site: str = Field(pattern="^(myvipon|rebaid|rebatekey)$")
    name: str
    schedule_cron: str = ""

class JobOut(BaseModel):
    id: int
    site: str
    name: str
    schedule_cron: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True
class SiteBrief(BaseModel):
    id: int
    name: str
    class Config:
        from_attributes = True

class ProductOut(BaseModel):
    id: int
    site: SiteBrief
    product_url: str
    type: Optional[str] = None
    title: Optional[str] = None
    price: Optional[Decimal] = None
    image_url: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    amazon_url: Optional[str] = None
    amazon_store_url: Optional[str] = None
    amazon_store_name: Optional[str] = None
    external_id: Optional[str] = None
    first_seen_at: datetime
    last_seen_at: datetime
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class ProductPage(BaseModel):
    items: List[ProductOut]
    total: int
    page: int
    page_size: int
    has_next: bool
    has_prev: bool

class UserOut(BaseModel):
    id: int
    email: EmailStr
    role: str
    is_active: bool
    class Config:
        from_attributes = True

class UserCreateAdmin(BaseModel):
    email: EmailStr
    password: str
    role: str = "viewer"  # "viewer" | "admin" | "superuser"

class UserUpdateAdmin(BaseModel):
    password: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None

class UserRow(BaseModel):
    id: int
    email: EmailStr
    role: str
    is_active: bool
    class Config:
        from_attributes = True

class UserPage(BaseModel):
    items: list[UserRow]
    total: int
    page: int
    page_size: int

class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"

class ProfileUpdate(BaseModel):
    email: Optional[EmailStr] = None  # extend later (name, avatar, etc.)

class PasswordChangeIn(BaseModel):
    current_password: str
    new_password: str
    new_password_confirm: Optional[str] = None

class ProfileResponse(BaseModel):
    user: "UserOut"                     # forward ref; UserOut already defined above
    access_token: Optional[str] = None  # new JWT if identity changed/rotated
    token_type: Optional[str] = "bearer"

class RebaidCategoryIn(BaseModel):
    name: str
    url: HttpUrl

class RebaidScrapeIn(BaseModel):
    categories: List[RebaidCategoryIn]
    max_pages: int = 0
    timeout_ms: int = 30000
    delay_min: float = 0.15
    delay_max: float = 0.45