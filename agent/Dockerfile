# 빌드 컨텍스트는 레포 루트: docker build -f agent/Dockerfile -t kkori-agent .
FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# 의존성 레이어 — 소스 변경 시에도 캐시 유지
COPY agent/pyproject.toml agent/uv.lock ./
RUN uv sync --locked --no-dev

ENV PATH="/app/.venv/bin:$PATH"

COPY shared/ shared/
COPY agent/src/ src/

RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/.hf && chown appuser:appuser /app/.hf
USER appuser

# 로컬 inference 모델(턴 디텍터 등)을 빌드 시점에 캐시 — 컨테이너 첫 기동 시 다운로드 지연 방지
ENV HF_HOME=/app/.hf
RUN python -m src.main download-files

CMD ["python", "-m", "src.main", "start"]
