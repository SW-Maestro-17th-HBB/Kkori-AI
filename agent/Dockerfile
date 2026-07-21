# 빌드 컨텍스트는 레포 루트: docker build -f agent/Dockerfile -t kkori-agent .
FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# 의존성 레이어 — 소스 변경 시에도 캐시 유지
COPY pyproject.toml uv.lock ./
COPY agent/pyproject.toml agent/
COPY worker/pyproject.toml worker/
RUN uv sync --frozen --no-dev --package kkori-agent

ENV PATH="/app/.venv/bin:$PATH"

COPY shared/ shared/
COPY agent/src/ src/

RUN useradd --create-home --uid 10001 appuser
USER appuser

CMD ["python", "-m", "src.main", "start"]
