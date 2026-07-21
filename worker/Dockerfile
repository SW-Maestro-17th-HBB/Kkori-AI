#   docker build -f worker/Dockerfile -t kkori-worker .
FROM python:3.13-slim

# uv 바이너리 (공식 배포 이미지에서 복사)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

# 의존성 레이어 — 코드보다 먼저 복사해 Docker 캐시 활용
COPY worker/pyproject.toml worker/uv.lock ./
RUN uv sync --locked --no-dev

COPY shared/ shared/
COPY worker/src/ src/

ENV PATH="/app/.venv/bin:$PATH"
CMD ["faststream", "run", "src.main:app"]
