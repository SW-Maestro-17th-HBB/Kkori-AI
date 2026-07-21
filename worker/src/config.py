"""워커 설정 — 환경변수/.env 로 주입한다.

접두사 `KKORI_WORKER_` 를 붙인 환경변수로 아래 값을 덮어쓴다
(예: `KKORI_WORKER_DELIVERY_COUNT_THRESHOLD=5`).
기본값은 로컬 docker-compose(Postgres 5432 / Redis 6379 / MinIO 9000)에 맞춰져 있어
로컬에선 그대로 동작하고, dev/prod 는 환경변수로 주입한다.

관련: PRD §8(AI/인프라), §9(재량 파라미터), §2.5(청킹).
"""

from __future__ import annotations

import socket
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="KKORI_WORKER_",
        env_file=".env",
        extra="ignore",
    )

    # --- Redis (FastStream 브로커 + Stream 소비) ---
    redis_url: str = "redis://localhost:6379"

    # --- PostgreSQL (resume_chunks·상태 직접 기록) ---
    postgres_dsn: str = "postgresql://kkori:kkori@localhost:5432/kkori"

    # --- S3 / MinIO (이력서 PDF 다운로드) ---
    # 로컬은 MinIO 엔드포인트, 실서버는 빈값으로 두면 boto3 기본 동작(실제 S3 + IAM Role).
    s3_endpoint_url: str = "http://localhost:9000"
    s3_region: str = "ap-northeast-2"
    # 자격증명 — 빈값이면 boto3 기본 체인(IAM Role·환경변수·~/.aws)에 맡긴다.
    # 로컬 MinIO 는 .env 로 주입 (docker-compose 기본: kkori / kkori1234).
    s3_access_key: str = ""
    s3_secret_key: str = ""

    # --- AI 제공자 선택 (§8) ---
    # "fake" = 가짜(로컬/테스트, 클라우드 없이) / "bedrock" = 실제(클라우드 준비 후)
    ai_provider: str = "fake"

    # --- AWS Bedrock (LLM·임베딩) --- (실 호출 탐침으로 확정, 2026-07-21)
    # 계정이 소속된 조직의 SCP 가 Bedrock 을 us-east-1 에서만 허용한다(서울·global 프로파일 전부 거부).
    bedrock_region: str = "us-east-1"
    structuring_model_id: str = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
    # Embed v4 — 출력 차원을 1024 로 지정해 vector(1024) 스키마를 유지한다 (§8)
    embedding_model_id: str = "cohere.embed-v4:0"
    embedding_dim: int = 1024  # 임베딩 모델의 출력 차원 설정에 종속 — 변경 시 스키마도 조정

    # --- Redis Stream 소비 (§9) ---
    consumer_group: str = "kkori-worker"
    consumer_name: str = ""  # 빈값이면 런타임에 hostname 사용(인스턴스 구분)

    # --- 재량 파라미터 (§9) ---
    delivery_count_threshold: int = 3  # 초과 시 재처리 없이 FAILED (§4)
    claim_min_idle_ms: int = 300_000  # XAUTOCLAIM 회수 대상 판정(5분)
    reclaim_interval_s: int = 300  # 회수 루프 주기(5분) — 최악 복구 지연 = min_idle + 주기 ≤ 10분
    reclaim_batch_size: int = 10  # 한 번의 XAUTOCLAIM 으로 가져올 최대 메시지 수
    retry_max_attempts: int = 3  # 외부 호출(S3·LLM·임베딩) 내부 재시도 최대 시도 수 (§9)
    retry_base_delay_s: float = 1.0  # 지수 백오프 시작 간격 — 1s → 2s → 4s

    # --- 청킹 (§2.5) ---
    chunk_target_tokens: int = 512  # 초과 엔티티만 문장 경계로 분할
    chunk_overlap_sentences: int = 1  # 분할 조각 간 겹침
    chunk_version: int = 3  # metadata.chunk_version — 색인 스키마 버전 (3 = 성과 단위 청킹+풍부화)

    @property
    def resolved_consumer_name(self) -> str:
        """consumer_name 이 비어 있으면 hostname 으로 대체."""
        return self.consumer_name or socket.gethostname()

    @property
    def s3_endpoint(self) -> str | None:
        """빈 문자열은 None 으로(실서버 = 기본 S3 엔드포인트)."""
        return self.s3_endpoint_url or None


@lru_cache
def get_settings() -> Settings:
    """설정 싱글턴. 최초 호출 시 환경변수를 읽어 캐시한다."""
    return Settings()
