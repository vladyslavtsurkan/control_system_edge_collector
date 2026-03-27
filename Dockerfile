# ── Build stage ──────────────────────────────────────────
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11.2 /uv /uvx /bin/

WORKDIR /build
COPY pyproject.toml uv.lock ./

RUN uv export --format requirements-txt --locked --no-dev --no-emit-project --no-hashes -o requirements.txt \
    && cat requirements.txt \
    && pip install --no-cache-dir --target=/install -r requirements.txt

# ── Runtime stage ────────────────────────────────────────
FROM python:3.12-slim AS runtime

ENV PYTHONPATH="/install:${PYTHONPATH}"
ENV PATH="/install/bin:${PATH}"

# Non-root user for security
RUN groupadd --gid 1000 collector \
    && useradd --uid 1000 --gid collector --shell /bin/bash --create-home collector

COPY --from=builder /install /install

WORKDIR /home/collector/app
COPY app/ ./app/
COPY scripts/ ./scripts/

RUN chown -R collector:collector /home/collector/app

USER collector

ENTRYPOINT ["python", "-m", "app.main"]
