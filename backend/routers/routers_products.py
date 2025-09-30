from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_, asc, desc
from sqlalchemy.orm import joinedload
from db import get_session
import models
from schemas import ProductOut, ProductPage
from security import get_current_user
from typing import Literal

router = APIRouter(prefix="/products", tags=["products"])

SORT_MAP = {
    "id": models.Product.id,
    "created_at": models.Product.created_at,
    "last_seen_at": models.Product.last_seen_at,
    "price": models.Product.price,
}

@router.get("", response_model=ProductPage, dependencies=[Depends(get_current_user)])
def list_products(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    site: Optional[str] = Query(None, description="Filter by site name: myvipon|rebaid|rebatekey"),
    q: Optional[str] = Query(None, description="Search in title / product_url / store name"),
    sort: str = Query("-created_at", description="Field to sort by, prefix with '-' for desc"),
    store: Literal["any", "present", "missing"] = Query("any", description="filter by amazon store presence"),
    db: Session = Depends(get_session),
):
    # base query with site eager-loaded to avoid N+1
    query = db.query(models.Product).options(joinedload(models.Product.site))

    # filter by site name if provided
    if site:
        query = query.join(models.Site).filter(models.Site.name == site)

    # simple search
    if q:
        like = f"%{q}%"
        query = query.filter(
            or_(
                models.Product.title.ilike(like),
                models.Product.product_url.ilike(like),
                models.Product.amazon_store_name.ilike(like),
                models.Product.category.ilike(like),
            )
        )

    # store presence filter
    if store == "present":
        query = query.filter(
            and_(
                models.Product.amazon_store_name.is_not(None),
                models.Product.amazon_store_name != "",
                models.Product.amazon_store_url.is_not(None),
                models.Product.amazon_store_url != "",
            )
        )
    elif store == "missing":
        query = query.filter(
            or_(
                models.Product.amazon_store_name.is_(None),
                models.Product.amazon_store_name == "",
                models.Product.amazon_store_url.is_(None),
                models.Product.amazon_store_url == "",
            )
        )

    # total before pagination
    total = query.count()

    # sorting
    direction = desc if sort.startswith("-") else asc
    key = sort.lstrip("+-")
    col = SORT_MAP.get(key, models.Product.created_at)
    query = query.order_by(direction(col))

    # pagination
    items = query.offset((page - 1) * page_size).limit(page_size).all()

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "has_next": (page * page_size) < total,
        "has_prev": page > 1,
    }

@router.get("/{product_id}", response_model=ProductOut, dependencies=[Depends(get_current_user)])
def get_product(product_id: int, db: Session = Depends(get_session)):
    obj = (
        db.query(models.Product)
        .options(joinedload(models.Product.site))
        .filter(models.Product.id == product_id)
        .first()
    )
    if not obj:
        raise HTTPException(status_code=404, detail="Product not found")
    return obj
