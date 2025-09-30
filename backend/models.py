from sqlalchemy import (
    String, Integer, Boolean, DateTime, ForeignKey, Text, Numeric,
    UniqueConstraint, Index,func, JSON
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, backref
from datetime import datetime
from decimal import Decimal
from db import Base
from enum import Enum
from sqlalchemy import Enum as SAEnum  # <-- use SQLAlchemy's Enum

class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (UniqueConstraint("name", name="uq_jobs_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), index=True)  # e.g. "prep_full_fresh_run"
    schedule_cron: Mapped[str] = mapped_column(String(100), default="")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

jobrun_status = SAEnum(
    "queued", "running", "done", "error", "canceled",
    name="jobrun_status", validate_strings=True
)
site_enum = SAEnum(
    "rebaid", "rebatekey", "myvipon",
    name="site_enum", validate_strings=True
)
stage_enum = SAEnum(
    "urls", "details",
    name="stage_enum", validate_strings=True
)

class JobRun(Base):
    __tablename__ = "job_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
    status: Mapped[str] = mapped_column(jobrun_status, default="queued", index=True)

    queued_at:   Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    started_at:  Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    total:      Mapped[int] = mapped_column(Integer, default=0)
    processed:  Mapped[int] = mapped_column(Integer, default=0)
    ok_count:   Mapped[int] = mapped_column(Integer, default=0)
    fail_count: Mapped[int] = mapped_column(Integer, default=0)

    note:  Mapped[str] = mapped_column(String(500), default="")
    meta:  Mapped[dict] = mapped_column(JSON, default=dict)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    job: Mapped["Job"] = relationship(backref=backref("runs", cascade="all, delete-orphan"))

class JobRunPart(Base):
    __tablename__ = "job_run_parts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("job_runs.id", ondelete="CASCADE"), index=True)

    site: Mapped[str] = mapped_column(site_enum, index=True)      # per-site tracking
    stage: Mapped[str] = mapped_column(stage_enum, default="urls")
    status: Mapped[str] = mapped_column(jobrun_status, default="queued", index=True)

    total:      Mapped[int] = mapped_column(Integer, default=0)
    processed:  Mapped[int] = mapped_column(Integer, default=0)
    ok_count:   Mapped[int] = mapped_column(Integer, default=0)
    fail_count: Mapped[int] = mapped_column(Integer, default=0)

    note: Mapped[str] = mapped_column(String(500), default="")
    meta: Mapped[dict] = mapped_column(JSON, default=dict)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    run: Mapped["JobRun"] = relationship(backref=backref("parts", cascade="all, delete-orphan"))

class JobEvent(Base):
    __tablename__ = "job_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("job_runs.id", ondelete="CASCADE"), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    level: Mapped[str] = mapped_column(String(10), default="info")
    message: Mapped[str] = mapped_column(String(400), default="")
    plus: Mapped[int] = mapped_column(Integer, default=0)  # processed delta
    meta: Mapped[dict] = mapped_column(JSON, default=dict)

    run = relationship("JobRun", backref=backref("events", cascade="all, delete-orphan"))

class Site(Base):
    __tablename__ = "sites"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, index=True)   # 'myvipon'|'rebaid'|'rebatekey'
    base_url: Mapped[str | None] = mapped_column(String(255), nullable=True)

    products: Mapped[list["Product"]] = relationship(
        back_populates="site", cascade="all, delete-orphan"
    )

class Product(Base):
    __tablename__ = "products"

    # required bits for stage 1 (URL collection)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="RESTRICT"), index=True, nullable=False)
    product_url: Mapped[str] = mapped_column(Text, nullable=False, unique=True)  # unique across all sites
    type: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    # stage 2 (details) — all nullable
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)  # USD
    image_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    category: Mapped[str | None] = mapped_column(String(120), index=True, nullable=True)
    amazon_url: Mapped[str | None] = mapped_column(Text, index=True, nullable=True)
    amazon_store_url: Mapped[str | None] = mapped_column(Text, index=True, nullable=True)
    amazon_store_name: Mapped[str | None] = mapped_column(String(200), index=True, nullable=True)

    # optional helpers (nullable so you can fill later if you want)
    external_id: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)

    # timestamps (keep system timestamps non-null with defaults)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    site: Mapped["Site"] = relationship(back_populates="products")

    __table_args__ = (
        # Unique product_url covers dedupe between stage 1 & 2
        UniqueConstraint("product_url", name="uq_products_url"),
        # Helpful indexes
        Index("ix_site_last_seen", "site_id", "last_seen_at"),
    )
class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(32), default="admin")  # or "viewer"
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())