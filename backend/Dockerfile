# backend/Dockerfile
FROM mcr.microsoft.com/playwright/python:v1.47.2-jammy

WORKDIR /app

# Python deps
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir "playwright==1.47.2"

# (Browsers already in the base image.)
# If you want to be explicit (harmless):
# RUN playwright install chromium

# App code
COPY backend/ .

ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
