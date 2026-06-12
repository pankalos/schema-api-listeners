FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir requests paho-mqtt

COPY leaf-listener.py /app/leaf-listener.py

# IMPORTANT: use CMD, NOT ENTRYPOINT
# This prevents duplication when schema-api provides ["python","-u","leaf-listener.py", ...]
CMD ["python", "-u", "/app/leaf-listener.py"]

