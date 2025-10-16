# routers/exports.py
from __future__ import annotations

import csv
import json
import logging
import random
import re
import time
from datetime import datetime, time as dtime
from io import StringIO
from typing import Iterable, List, Optional, Literal, Iterator, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import and_
from sqlalchemy.orm import Session

from db import get_session
import models
from security import get_current_user
from settings import settings

# Google API errors type (used by retry helpers)
try:
    from googleapiclient.errors import HttpError  # type: ignore
except Exception:  # library may not be installed on app boot; we only use it in Sheets code-paths
    HttpError = Exception  # fallback typing

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
            return datetime.combine(d, dtime.max if end else dtime.min)
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


def _parse_a1_cell(cell: str) -> Tuple[int, str]:
    """
    Parse an A1 cell like 'B3' -> (row=3, col_letter='B').
    Defaults to (1, 'A') if parsing fails.
    """
    try:
        m = re.match(r"^\s*([A-Za-z]+)(\d+)\s*$", cell or "")
        if not m:
            return 1, "A"
        col_letters = m.group(1).upper()
        row_num = int(m.group(2))
        if row_num < 1:
            row_num = 1
        return row_num, col_letters
    except Exception:
        return 1, "A"

# ------------------ RETRYABLE SHEETS OPS (avoid 500s) -------------------------
def _retryable(exc: Exception) -> bool:
    status = getattr(getattr(exc, "resp", None), "status", None)
    msg = (str(exc) or "").lower()
    return (
        status == 429 or (isinstance(status, int) and status >= 500)
        or "internal error" in msg or "backend" in msg
    )


def _exec_with_retry(fn, tries: int = 6, base: float = 0.4):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if i == tries - 1 or not _retryable(e):
                raise
            time.sleep(base * (2 ** i) + random.random() * 0.2)
    if last:
        raise last


def _ensure_worksheet(service, spreadsheet_id: str, title: Optional[str]) -> str:
    """
    Return an existing or created sheet title.
    If title is None, return the first sheet title.
    """
    meta = _exec_with_retry(lambda: service.spreadsheets().get(
        spreadsheetId=spreadsheet_id, fields="sheets.properties"
    ).execute())
    sheets = meta.get("sheets", [])
    if not sheets:
        raise HTTPException(status_code=400, detail="Spreadsheet has no sheets")
    if title:
        for s in sheets:
            if s["properties"]["title"] == title:
                return title
        # create if missing
        _exec_with_retry(lambda: service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
        ).execute())
        return title
    return sheets[0]["properties"]["title"]


def _clear_tab(service, spreadsheet_id: str, title: str, last_col_letter: str) -> None:
    _exec_with_retry(lambda: service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{title}!A:{last_col_letter}",
        body={}
    ).execute())


def _write_chunk(service, spreadsheet_id: str, title: str, start_cell_a1: str, rows2d: List[List[str]]):
    body = {
        "valueInputOption": "RAW",
        "data": [{
            "range": f"{title}!{start_cell_a1}",
            "majorDimension": "ROWS",
            "values": rows2d,
        }],
    }
    _exec_with_retry(lambda: service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute())


def _get_last_non_empty_row(service, spreadsheet_id: str, title: str) -> int:
    """
    Returns the last non-empty row index by scanning column A.
    If there is no data at all, returns 0.
    """
    resp = _exec_with_retry(lambda: service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{title}!A:A"
    ).execute())
    values = resp.get("values", [])
    return len(values)  # number of non-empty rows in col A


def _header_exists(service, spreadsheet_id: str, title: str) -> bool:
    col = _col_letter(len(CSV_FIELDS))
    resp = _exec_with_retry(lambda: service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{title}!A1:{col}1"
    ).execute())
    vals = resp.get("values", [])
    if not vals:
        return False
    row = vals[0]
    # If the header row has the same number of columns and identical values, consider present.
    return len(row) >= len(CSV_FIELDS) and all((row[i] == CSV_FIELDS[i] for i in range(len(CSV_FIELDS))))

# ------------------ STREAMED WRITERS -------------------------
def _iter_rows_from_query(q, batch_size: int = 5000) -> Iterator[models.Product]:
    # SQLAlchemy stream: avoid loading all rows into RAM
    for p in q.yield_per(batch_size):
        yield p


def _write_replace_streamed(
    service, spreadsheet_id: str, title: str, start_cell: str, row_iter: Iterator[models.Product],
    *, chunk_rows: int = 1000
) -> int:
    """
    Clears the sheet, writes header at start_cell, then streams rows in chunks below it.
    Returns total written rows (including header).
    """
    start_row, start_col_letters = _parse_a1_cell(start_cell)
    last_col_letter = _col_letter(len(CSV_FIELDS))

    # clear entire data region (A:ZZ) to keep behavior consistent with original "replace"
    _clear_tab(service, spreadsheet_id, title, last_col_letter)

    # write header at start_cell (e.g., A1 or custom)
    _write_chunk(service, spreadsheet_id, title, f"{start_col_letters}{start_row}", [CSV_FIELDS])
    written = 1
    next_row = start_row + 1

    buf: List[List[str]] = []
    for p in row_iter:
        buf.append(_row_from_product(p))
        if len(buf) >= chunk_rows:
            _write_chunk(service, spreadsheet_id, title, f"{start_col_letters}{next_row}", buf)
            written += len(buf)
            next_row += len(buf)
            buf = []
    if buf:
        _write_chunk(service, spreadsheet_id, title, f"{start_col_letters}{next_row}", buf)
        written += len(buf)

    return written


def _write_append_streamed(
    service, spreadsheet_id: str, title: str, row_iter: Iterator[models.Product],
    *, chunk_rows: int = 1000
) -> int:
    """
    Appends to the end of the sheet (based on column A).
    If the sheet is empty, writes the header first.
    Returns total written rows (includes header if written).
    """
    written = 0
    last_row = _get_last_non_empty_row(service, spreadsheet_id, title)

    # If empty sheet: write header at A1, start at A2
    if last_row == 0:
        _write_chunk(service, spreadsheet_id, title, "A1", [CSV_FIELDS])
        written += 1
        next_row = 2
    else:
        # If sheet has data but header is missing (custom sheet), we don't inject header;
        # we just continue appending after last_row.
        next_row = last_row + 1

    buf: List[List[str]] = []
    for p in row_iter:
        buf.append(_row_from_product(p))
        if len(buf) >= chunk_rows:
            _write_chunk(service, spreadsheet_id, title, f"A{next_row}", buf)
            written += len(buf)
            next_row += len(buf)
            buf = []
    if buf:
        _write_chunk(service, spreadsheet_id, title, f"A{next_row}", buf)
        written += len(buf)

    return written

# ------------------ API MODEL -------------------------
class SheetExportBody(BaseModel):
    spreadsheet_id: str
    worksheet: Optional[str] = None
    mode: Literal["replace", "append"] = "replace"
    start_cell: str = "A1"   # honored for replace; append always appends after last row

# ------------------ EXPORT ENDPOINT -------------------------
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

    # Stream the DB rows
    rows_iter = _iter_rows_from_query(q, batch_size=5000)

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

    # ----- write values (streamed & chunked) -----
    try:
        if body.mode == "append":
            written_rows = _write_append_streamed(
                service=service,
                spreadsheet_id=body.spreadsheet_id,
                title=sheet_title,
                row_iter=rows_iter,
                chunk_rows=1000,  # tune if needed
            )
        else:
            written_rows = _write_replace_streamed(
                service=service,
                spreadsheet_id=body.spreadsheet_id,
                title=sheet_title,
                start_cell=body.start_cell or "A1",
                row_iter=rows_iter,
                chunk_rows=1000,  # tune if needed
            )
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
        "written_rows": written_rows,  # includes header row if written
        "service_account": sa_email,
    }
