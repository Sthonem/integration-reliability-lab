# One image for the hub API, the worker, the migration job and the three mock providers.
FROM python:3.13-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY irlab ./irlab
RUN pip install .
COPY alembic.ini ./
COPY migrations ./migrations
RUN useradd --create-home --uid 10001 irlab
USER irlab
EXPOSE 8000
HEALTHCHECK --interval=5s --timeout=3s --retries=12 \
  CMD python -c "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).status == 200 else 1)"
CMD ["uvicorn", "irlab.main:app", "--host", "0.0.0.0", "--port", "8000"]

# Test image: the same code plus the test suite, run against the Compose Postgres and Redis
# (`docker compose --profile test run --rm tests`).
FROM runtime AS test
USER root
RUN pip install ".[dev,ai]"
COPY tests ./tests
COPY .env.example ./
RUN chown -R irlab /app
USER irlab
HEALTHCHECK NONE
CMD ["pytest", "-q"]
