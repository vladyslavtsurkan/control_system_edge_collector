# ── Build stage ──────────────────────────────────────────
FROM python:3.12-slim AS builder

RUN pip install --no-cache-dir poetry poetry-plugin-export

WORKDIR /build
COPY pyproject.toml poetry.lock ./

RUN poetry export -f requirements.txt --without-hashes -o requirements.txt \
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
