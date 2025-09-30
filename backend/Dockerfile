# backend/Dockerfile
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# System deps needed for Playwright HTTP client + Chromium runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl wget gnupg \
    # chromium runtime libs
    libnss3 libnspr4 libatk-bridge2.0-0 libatk1.0-0 libcups2 libdrm2 libdbus-1-3 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libasound2 \
    libatspi2.0-0 libwayland-client0 libwayland-egl1 libwayland-server0 \
    libgtk-3-0 libpango-1.0-0 libpangocairo-1.0-0 libcairo2 libgdk-pixbuf2.0-0 \
    # fonts (modern package names; avoids the ttf-* issue)
    fonts-liberation fonts-unifont fonts-ubuntu \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir playwright

# Install browsers (no --with-deps since we installed OS deps ourselves)
# If your code only uses request.new_context (no browser launch), this is optional.
RUN playwright install chromium

# App code
COPY backend/ .

# If you run as root, Playwright requests work fine.
# If you later launch the real browser (not just requests), you may need --no-sandbox in your code.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
