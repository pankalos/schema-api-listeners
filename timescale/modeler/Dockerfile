# Dockerfile
FROM python:3.11-slim

# Make logs show up immediately
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install only what we need
RUN pip install --no-cache-dir Flask==3.0.0

# Copy your app
COPY modeler.py .

# The app listens on 8080 already
EXPOSE 8080

# Run it
CMD ["python", "modeler.py"]
