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

    # --- AWS Bedrock (LLM·임베딩) ---
    # 서울 리전 모델 가용성 제한 가능 → 기본 us-east-1 (§8).
    bedrock_region: str = "us-east-1"
    # TODO: 정확한 Bedrock 모델/추론 프로파일 ID로 확인·교체 (콘솔 기준).
    structuring_model_id: str = "anthropic.claude-haiku-4-5"
    embedding_model_id: str = "cohere.embed-multilingual-v3"
    embedding_dim: int = 1024  # Cohere v3 에 종속 — 모델 교체 시 함께 조정

    # --- Redis Stream 소비 (§9) ---
    consumer_group: str = "kkori-worker"
    consumer_name: str = ""  # 빈값이면 런타임에 hostname 사용(인스턴스 구분)

    # --- 재량 파라미터 (§9) ---
    delivery_count_threshold: int = 3  # 초과 시 재처리 없이 FAILED (§4)
    claim_min_idle_ms: int = 300_000  # XAUTOCLAIM 회수 대상 판정(5분)

    # --- 청킹 (§2.5) ---
    chunk_target_tokens: int = 512  # 초과 엔티티만 문장 경계로 분할
    chunk_overlap_sentences: int = 1  # 분할 조각 간 겹침
    chunk_version: int = 1  # metadata.chunk_version — 전략 교체 시 백필 대상 식별

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
