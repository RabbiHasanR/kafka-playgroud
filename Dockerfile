# One image for the order service and all three consumers — they differ only in the
# command and the environment compose gives them, not in their dependencies.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src

RUN pip install --no-cache-dir .

# Overridden per service in docker-compose.yml.
CMD ["python", "-m", "order_service.consumer.main"]
