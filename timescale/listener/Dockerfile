FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

# ENV PYTHONUNBUFFERED=1  \
#     PYTHONDONTWRITEBYTECODE=1 \
#     PIP_NO_CACHE_DIR=1

# Set the working directory inside the container
WORKDIR /app

# Install Python deps. Pin if you can.
# psycopg[binary] pulls prebuilt wheels on slim (good).
RUN pip install --no-cache-dir "requests>=2.32" "psycopg[binary]>=3.2"

# Copy the scripts and necessary files into the container
COPY timescaledb-listener.py /app/timescaledb-listener.py

# Create non-root user
# Create a regular user with fixed UID 10001 (no -r, so no SYS_UID_MAX warning)
RUN useradd -u 10001 -m appuser
USER appuser

# Use ENTRYPOINT so K8s can pass flags via args:
# ENTRYPOINT ["python", "/app/timescaledb-listener.py"]

CMD ["python", "/app/timescaledb-listener.py"]

