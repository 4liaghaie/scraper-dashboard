# backend/Dockerfile
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# System libs for Playwright HTTP client / Chromium runtime (safe even if you don't launch a browser)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl wget gnupg \
    libnss3 libnspr4 libatk-bridge2.0-0 libatk1.0-0 libcups2 libdrm2 libdbus-1-3 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libasound2 \
    libatspi2.0-0 libwayland-client0 libwayland-egl1 libwayland-server0 \
    libgtk-3-0 libpango-1.0-0 libpangocairo-1.0-0 libcairo2 \
    libgdk-pixbuf-2.0-0 libgbm1 libxshmfence1 libu2f-udev libvulkan1 \
    fonts-liberation fonts-unifont fonts-noto-core fonts-noto-color-emoji fonts-dejavu-core \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir playwright

# Install browser binaries only (do NOT use --with-deps, it re-runs apt and breaks on ttf-*)
RUN playwright install chromium

# App
COPY backend/ .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
