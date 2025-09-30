FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# System deps (chromium libs for Playwright's request context)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-0 \
    libdrm2 libdbus-1-3 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libgtk-3-0 libcups2 libnss3 libnspr4 libx11-6 \
    libx11-xcb1 libxext6 libxshmfence1 libu2f-udev libvulkan1 \
  && rm -rf /var/lib/apt/lists/*

# Python deps
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir "playwright==1.44.0" "psycopg2-binary"

# Install chromium (and its deps) for Playwright
RUN playwright install --with-deps chromium

# App code
COPY backend/ .

# Uvicorn
ENV PORT=8000
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host","0.0.0.0","--port","8000","--proxy-headers","--forwarded-allow-ips","*"]
