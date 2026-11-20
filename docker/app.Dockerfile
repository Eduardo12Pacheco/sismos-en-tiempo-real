# syntax=docker/dockerfile:1
# One application image for bootstrap, ingest, processor, sink, monitor and cli
# (D19): multi-stage uv build, non-root runtime, pre-created state directory.

FROM python:3.12-slim-bookworm AS builder
COPY --from=ghcr.io/astral-sh/uv:0.11.29 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock README.md README.es.md LICENSE ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.12-slim-bookworm AS runtime
LABEL org.opencontainers.image.title="realtime-seismic-stream" \
      org.opencontainers.image.description="Local real-time earthquake streaming pipeline" \
      org.opencontainers.image.licenses="MIT"
RUN groupadd --system --gid 10001 seismic \
    && useradd --system --uid 10001 --gid seismic --home-dir /app --shell /usr/sbin/nologin seismic \
    && mkdir -p /var/lib/seismic/state \
    && chown -R seismic:seismic /var/lib/seismic
WORKDIR /app
COPY --from=builder --chown=seismic:seismic /app/.venv /app/.venv
# Bundled offline replay sample (S1) so `ingest` works without host mounts.
COPY --chown=seismic:seismic data ./data
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SEISMIC_STATE_DIR=/var/lib/seismic/state
USER seismic
ENTRYPOINT ["seismic-stream"]
