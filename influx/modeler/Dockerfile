FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Only what modeler.py needs
RUN pip install --no-cache-dir flask

COPY modeler.py /app/modeler.py

EXPOSE 8080

CMD ["python", "/app/modeler.py"]
