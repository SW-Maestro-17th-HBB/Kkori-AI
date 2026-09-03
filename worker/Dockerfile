#   docker build -f worker/Dockerfile -t kkori-worker .
FROM python:3.13-slim

# uv 바이너리 (공식 배포 이미지에서 복사)
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /uvx /usr/local/bin/

WORKDIR /app

# 의존성 레이어 — 코드보다 먼저 복사해 Docker 캐시 활용
COPY worker/pyproject.toml worker/uv.lock ./
RUN uv sync --locked --no-dev

COPY shared/ shared/
COPY worker/src/ src/

ENV PATH="/app/.venv/bin:$PATH"
# 동기 디스패치 실험(PRD §11)용 HTTP — 리포트 워커는 이 CMD 를 override 해 무영향
EXPOSE 8000
CMD ["faststream", "run", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
