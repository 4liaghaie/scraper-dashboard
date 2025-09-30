from logging.config import fileConfig
from alembic import context
from sqlalchemy import create_engine, pool

# --- make app package importable (flat layout) ---
import os, sys
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # ...\backend\alembic
APP_DIR = os.path.dirname(BASE_DIR)                    # ...\backend
if APP_DIR not in sys.path:
    sys.path.append(APP_DIR)

# Import your app bits
from settings import settings           # reads .env -> DATABASE_URL
from db import Base                     # DeclarativeBase
import models                           # ensure models are imported so Alembic "sees" them

# Alembic Config
config = context.config

# Logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Target metadata for autogenerate
target_metadata = Base.metadata

def run_migrations_offline():
    """Run migrations in 'offline' mode."""
    url = settings.database_url
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    """Run migrations in 'online' mode."""
    connectable = create_engine(
        settings.database_url,
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
