# routers/exports.py
from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, time
from io import StringIO
from typing import Iterable, List, Optional, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import and_
from sqlalchemy.orm import Session

from db import get_session
import models
from security import get_current_user
from settings import settings

router = APIRouter(prefix="/exports", tags=["exports"])
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# CSV field order (also used for Sheets)
# -------------------------------------------------------------------
CSV_FIELDS = [
    "id", "site_id", "product_url", "type", "title", "price", "image_url", "description",
    "category", "amazon_url", "amazon_store_url", "amazon_store_name", "external_id",
    "first_seen_at", "last_seen_at", "created_at", "updated_at",
]


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def _parse_date_bound(s: Optional[str], *, end: bool = False) -> Optional[datetime]:
    if not s:
        return None
    try:
        if len(s) == 10:  # YYYY-MM-DD
            d = datetime.strptime(s, "%Y-%m-%d").date()
            return datetime.combine(d, time.max if end else time.min)
        return datetime.fromisoformat(s)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD or ISO-8601.")


def _row_from_product(p: models.Product) -> List[str]:
    def dt(v): return v.isoformat(timespec="seconds") if v else ""
    def s(v): return "" if v is None else str(v)
    return [
        s(p.id), s(p.site_id), s(p.product_url), s(p.type), s(p.title), s(p.price),
        s(p.image_url), s(p.description), s(p.category), s(p.amazon_url),
        s(p.amazon_store_url), s(p.amazon_store_name), s(p.external_id),
        dt(p.first_seen_at), dt(p.last_seen_at), dt(p.created_at), dt(p.updated_at),
    ]


def _base_query(db: Session):
    return db.query(models.Product)


def _apply_filters(
    q,
    *,
    site: Optional[str],
    site_id: Optional[int],
    type_: Optional[str],
    selected_ids: Optional[List[int]],
    last_seen_from: Optional[str],
    last_seen_to: Optional[str],
    limit: Optional[int],
):
    """
    Filter-only helper. No ordering here; ordering is applied in the endpoint.
    """
    if site:
        q = q.join(models.Site).filter(models.Site.name == site)
    if site_id:
        q = q.filter(models.Product.site_id == site_id)
    if type_:
        q = q.filter(models.Product.type == type_)
    if selected_ids:
        q = q.filter(models.Product.id.in_(selected_ids))

    start_dt = _parse_date_bound(last_seen_from, end=False)
    end_dt = _parse_date_bound(last_seen_to, end=True)
    if start_dt and end_dt and start_dt > end_dt:
        raise HTTPException(status_code=400, detail="last_seen_from must be <= last_seen_to")
    if start_dt and end_dt:
        q = q.filter(and_(models.Product.last_seen_at >= start_dt,
                          models.Product.last_seen_at <= end_dt))
    elif start_dt:
        q = q.filter(models.Product.last_seen_at >= start_dt)
    elif end_dt:
        q = q.filter(models.Product.last_seen_at <= end_dt)

    if limit:
        q = q.limit(limit)

    return q


def _stream_csv(rows: Iterable[models.Product]) -> Iterable[str]:
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(CSV_FIELDS)
    yield buf.getvalue()
    buf.seek(0); buf.truncate(0)

    for p in rows:
        writer.writerow(_row_from_product(p))
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)


# -------------------------------------------------------------------
# CSV endpoints
# -------------------------------------------------------------------
@router.get(
    "/products.csv",
    response_class=StreamingResponse,
    dependencies=[Depends(get_current_user)],
)
def export_products_csv(
    db: Session = Depends(get_session),
    site: Optional[str] = Query(None, description="Filter by site name (models.Site.name)"),
    site_id: Optional[int] = Query(None, description="Filter by site id"),
    last_seen_from: Optional[str] = Query(None, description="YYYY-MM-DD or ISO-8601"),
    last_seen_to: Optional[str] = Query(None, description="YYYY-MM-DD or ISO-8601"),
    type: Optional[str] = Query(None, description="Filter by Product.type"),
    id: Optional[int] = Query(None, description="Export a single product by id"),
    ids: Optional[str] = Query(None, description="Comma-separated product ids, e.g. 1,2,3"),
    limit: Optional[int] = Query(None, ge=1, le=200000, description="Limit number of rows"),
    sort: Optional[str] = Query("last_seen_desc", description="last_seen_desc|last_seen_asc"),
):
    # Selection handling
    selected_ids: List[int] = []
    if id is not None:
        selected_ids = [id]
    elif ids:
        try:
            selected_ids = [int(x.strip()) for x in ids.split(",") if x.strip()]
        except ValueError:
            raise HTTPException(status_code=400, detail="ids must be comma-separated integers")

    # Build the query with filters only
    q = _apply_filters(
        _base_query(db),
        site=site, site_id=site_id, type_=type,
        selected_ids=selected_ids,
        last_seen_from=last_seen_from, last_seen_to=last_seen_to,
        limit=limit,
    )

    # Apply sorting once
    s = (sort or "").lower()
    if s == "last_seen_desc":
        q = q.order_by(models.Product.last_seen_at.desc())
    elif s == "last_seen_asc":
        q = q.order_by(models.Product.last_seen_at.asc())
    else:
        q = q.order_by(models.Product.id.asc())

    rows = q.yield_per(1000)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"products_{timestamp}.csv"
    return StreamingResponse(
        _stream_csv(rows),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@router.get(
    "/products/{product_id}.csv",
    response_class=StreamingResponse,
    dependencies=[Depends(get_current_user)],
)
def export_single_product_csv(product_id: int, db: Session = Depends(get_session)):
    p = db.query(models.Product).filter(models.Product.id == product_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Product not found")

    def one():
        buf = StringIO()
        w = csv.writer(buf)
        w.writerow(CSV_FIELDS)
        w.writerow(_row_from_product(p))
        yield buf.getvalue()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"product_{product_id}_{timestamp}.csv"
    return StreamingResponse(
        one(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# -------------------------------------------------------------------
# Google Sheets export
# -------------------------------------------------------------------
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _load_service_account_creds():
    """
    Load service account credentials from settings.google_service_account_json.
    Value may be a JSON string or a file path to the JSON.
    """
    raw = getattr(settings, "google_service_account_json", None)
    if not raw:
        raise HTTPException(status_code=500, detail="GOOGLE_SERVICE_ACCOUNT_JSON is not set")

    try:
        from google.oauth2.service_account import Credentials  # type: ignore
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="Google Sheets export requires 'google-api-python-client' and 'google-auth'. "
                   "Install: pip install google-api-python-client google-auth",
        )

    try:
        if raw.strip().startswith("{"):
            info = json.loads(raw)
        else:
            with open(raw, "r", encoding="utf-8") as f:
                info = json.load(f)
        return Credentials.from_service_account_info(info, scopes=SHEETS_SCOPES)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Invalid service account JSON: {e}")


def _get_sheets_service(creds):
    """Lazy import the Sheets client to avoid import errors at app start."""
    try:
        from googleapiclient.discovery import build  # type: ignore
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="Google Sheets export requires 'google-api-python-client'. "
                   "Install it: pip install google-api-python-client",
        )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _ensure_worksheet(service, spreadsheet_id: str, title: Optional[str]) -> str:
    """
    Return an existing or created sheet title.
    If title is None, return the first sheet title.
    """
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = meta.get("sheets", [])
    if not sheets:
        raise HTTPException(status_code=400, detail="Spreadsheet has no sheets")
    if title:
        for s in sheets:
            if s["properties"]["title"] == title:
                return title
        # create if missing
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
        ).execute()
        return title
    return sheets[0]["properties"]["title"]


class SheetExportBody(BaseModel):
    spreadsheet_id: str
    worksheet: Optional[str] = None
    mode: Literal["replace", "append"] = "replace"
    start_cell: str = "A1"


@router.post("/products.google-sheet", dependencies=[Depends(get_current_user)])
def export_products_to_google_sheet(
    body: SheetExportBody = Body(...),
    db: Session = Depends(get_session),
    site: Optional[str] = Query(None),
    site_id: Optional[int] = Query(None),
    type: Optional[str] = Query(None),
    id: Optional[int] = Query(None, description="Export a single product by id"),
    ids: Optional[str] = Query(None, description="Comma-separated product ids, e.g. 1,2,3"),
    last_seen_from: Optional[str] = Query(None, description="YYYY-MM-DD or ISO-8601"),
    last_seen_to: Optional[str] = Query(None, description="YYYY-MM-DD or ISO-8601"),
    limit: Optional[int] = Query(None, ge=1, le=200000),
    # NEW: sorting control for Sheets export
    sort: Optional[str] = Query("last_seen_desc", description="last_seen_desc|last_seen_asc"),
):
    # ----- build selection -----
    selected_ids: List[int] = []
    if id is not None:
        selected_ids = [id]
    elif ids:
        try:
            selected_ids = [int(x.strip()) for x in ids.split(",") if x.strip()]
        except ValueError:
            raise HTTPException(status_code=400, detail="ids must be comma-separated integers")

    # ----- query data (filters only) -----
    q = _apply_filters(
        _base_query(db),
        site=site, site_id=site_id, type_=type,
        selected_ids=selected_ids,
        last_seen_from=last_seen_from, last_seen_to=last_seen_to,
        limit=limit,
    )

    # ----- apply sorting once -----
    s = (sort or "").lower()
    if s == "last_seen_desc":
        q = q.order_by(models.Product.last_seen_at.desc())
    elif s == "last_seen_asc":
        q = q.order_by(models.Product.last_seen_at.asc())
    else:
        q = q.order_by(models.Product.id.asc())

    rows = list(q.all())
    values: List[List[str]] = [CSV_FIELDS] + [_row_from_product(p) for p in rows]

    # ----- auth / service -----
    try:
        creds = _load_service_account_creds()
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to load Google service account creds")
        raise HTTPException(status_code=500, detail=f"Failed to load Google credentials: {e}")

    sa_email = getattr(creds, "service_account_email", "service account")

    try:
        service = _get_sheets_service(creds)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to build Sheets service")
        raise HTTPException(status_code=500, detail=f"Failed to initialize Google Sheets client: {e}")

    # ----- ensure worksheet -----
    try:
        sheet_title = _ensure_worksheet(service, body.spreadsheet_id, body.worksheet)
    except Exception as e:
        logger.exception("Failed to get/create worksheet")
        status = getattr(getattr(e, "resp", None), "status", None)
        if status in (403, 404):
            raise HTTPException(
                status_code=status,
                detail=(
                    "Google Sheets permission/ID issue. "
                    f"Verify the spreadsheet ID and share it with '{sa_email}' (Editor)."
                ),
            )
        raise HTTPException(status_code=500, detail=f"Failed to access spreadsheet: {e}")

    # ----- write values -----
    last_col_letter = _col_letter(len(CSV_FIELDS))
    updated_range: Optional[str] = None

    try:
        if body.mode == "append":
            resp = service.spreadsheets().values().append(
                spreadsheetId=body.spreadsheet_id,
                range=f"{sheet_title}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": values},
            ).execute()
            updated_range = (resp or {}).get("updates", {}).get("updatedRange")
        else:
            # replace: clear existing range then write from start_cell
            clear_range = f"{sheet_title}!A:{last_col_letter}"
            service.spreadsheets().values().clear(
                spreadsheetId=body.spreadsheet_id,
                range=clear_range,
                body={}
            ).execute()
            write_range = f"{sheet_title}!{body.start_cell}"
            resp = service.spreadsheets().values().update(
                spreadsheetId=body.spreadsheet_id,
                range=write_range,
                valueInputOption="RAW",
                body={"values": values},
            ).execute()
            updated_range = (resp or {}).get("updatedRange")
    except Exception as e:
        logger.exception("Failed to write to Google Sheet")
        status = getattr(getattr(e, "resp", None), "status", None)
        if status in (403, 404):
            raise HTTPException(
                status_code=status,
                detail=(
                    "Google Sheets write failed due to permission/ID. "
                    f"Share the sheet with '{sa_email}' and confirm the Spreadsheet ID."
                ),
            )
        raise HTTPException(status_code=500, detail=f"Google Sheets write failed: {e}")

    return {
        "spreadsheet_id": body.spreadsheet_id,
        "worksheet": sheet_title,
        "mode": body.mode,
        "written_rows": len(values),  # includes header row
        "updated_range": updated_range,
        "service_account": sa_email,
    }
