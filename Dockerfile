FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Configure via env / mounted config at runtime; do not bake secrets into the image.
CMD ["python", "-m" "snowpoly.realtime_prices"]
