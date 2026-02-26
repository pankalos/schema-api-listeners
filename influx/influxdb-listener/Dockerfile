FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir influxdb-client requests

COPY influxdb-listener.py /app/influxdb-listener.py

# IMPORTANT: use CMD, NOT ENTRYPOINT
# This prevents duplication when schema-api provides ["python","-u","influxdb-listener.py", ...]
CMD ["python", "-u", "/app/influxdb-listener.py"]