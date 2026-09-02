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

from pydantic import model_validator
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

    # --- 리포트 완성 판정 ---
    # 음성 분석 경로 도입 전에는 텍스트 평가만으로 리포트를 완성한다(백엔드 PRD:
    # "확정 전까지 Worker는 텍스트 3축으로만 동작하고 delivery_score는 null").
    # 음성 분석 소비자가 배포될 때 True 로 켜면 "텍스트·음성 둘 다 완료" 판정으로 바뀐다.
    audio_analysis_enabled: bool = False

    # --- AI 제공자 선택 (§8) ---
    # "fake" = 가짜(로컬/테스트, 클라우드 없이) / "bedrock" = 실제(클라우드 준비 후)
    ai_provider: str = "fake"

    # --- AWS Bedrock (LLM·임베딩) --- (실 호출 탐침으로 확정, 2026-07-21)
    # 계정이 소속된 조직의 SCP 가 Bedrock 을 us-east-1 에서만 허용한다(서울·global 프로파일 전부 거부).
    bedrock_region: str = "us-east-1"
    structuring_model_id: str = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
    # 리포트 답변 평가용 — 이력서 구조화와 같은 모델로 시작, 도메인별 독립 교체를 위해 분리
    evaluation_model_id: str = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
    # Embed v4 — 출력 차원을 1024 로 지정해 vector(1024) 스키마를 유지한다 (§8)
    embedding_model_id: str = "cohere.embed-v4:0"
    embedding_dim: int = 1024  # 임베딩 모델의 출력 차원 설정에 종속 — 변경 시 스키마도 조정

    # --- Redis Stream 소비 (§9) ---
    consumer_group: str = "kkori-worker"
    # 리포트 워커의 그룹 — 그룹은 스트림별 개념이라 같은 이름도 무방하지만,
    # 프로세스 분리 운영에서 모니터링·설정을 구분하기 위해 이름을 나눈다.
    report_consumer_group: str = "kkori-report-worker"
    consumer_name: str = ""  # 빈값이면 런타임에 hostname 사용(인스턴스 구분)

    # --- 재량 파라미터 (§9) ---
    delivery_count_threshold: int = 3  # 이상이면 재처리 없이 FAILED (§4 — 판정은 >=)
    claim_min_idle_ms: int = 300_000  # XAUTOCLAIM 회수 대상 판정(5분)
    # 회수 구독자의 폴링 간격 — 최악 복구 지연 = min_idle + 이 값.
    # min_idle 이 5분이라 지연 하한은 어차피 5분이므로, 더 촘촘히 돌려도 얻는 게 없고
    # XAUTOCLAIM 호출량만 늘어난다(100ms 면 하루 86만 회, 5s 면 1.7만 회).
    reclaim_poll_interval_ms: int = 5_000
    retry_max_attempts: int = 3  # 외부 호출(S3·LLM·임베딩) 내부 재시도 최대 시도 수 (§9)
    retry_base_delay_s: float = 1.0  # 지수 백오프 시작 간격 — 1s → 2s → 4s

    # --- 동기 디스패치 실험 (§11) ---
    # fake 제공자의 인위 지연 — 분석 1건당 총 지연 ≈ 이 값 (FakeEmbedder.embed_documents 에만
    # 적용, 임베딩 단계는 FULL/REINDEX 어느 경로든 종단 전 정확히 1회). 0 = 지연 없음(기존 동작).
    fake_delay_seconds: float = 0.0
    # 커넥션 풀 최대 연결 수 (§11.4) — HTTP·스트림 두 경로가 공유하므로 워커의 PG 연결
    # 총량이 이 값으로 묶인다. 동시 처리 상한이자 처리량 노브(≈ 동시성 ÷ 건당 처리 시간).
    # 단 실효 동시 추론은 to_thread 스레드풀 크기 min(32, cpu+4)와의 min — 측정 시 그 이하로.
    db_pool_max_size: int = 10
    # 풀 대기 타임아웃(초) — 연결이 전부 대출 중일 때 이만큼 기다린 뒤 HTTP 는 503,
    # 스트림은 예외 → PEL 회수. 대기 + 처리 시간이 Spring read timeout(120s) 안에 들도록.
    db_pool_wait_timeout_s: float = 60.0
    # 스트림 소비 동시 처리 수 (§11.4) — 1 = 순차(기존 동작). 풀 크기 이하여야 한다
    # (초과하면 연결 대기로 동시성이 조용히 풀 크기로 깎이므로 기동 시 검증).
    stream_max_workers: int = 1

    # --- 청킹 (§2.5) ---
    chunk_target_tokens: int = 512  # 초과 엔티티만 문장 경계로 분할
    chunk_overlap_sentences: int = 1  # 분할 조각 간 겹침
    chunk_version: int = 3  # metadata.chunk_version — 색인 스키마 버전 (3 = 성과 단위 청킹+풍부화)

    @model_validator(mode="after")
    def _validate_workers_within_pool(self) -> "Settings":
        """N ≤ P 기동 검증 (§11.4) — 잘못된 조합이 조용히 성능만 깎는 일을 막는다."""
        if self.stream_max_workers > self.db_pool_max_size:
            raise ValueError(
                f"stream_max_workers({self.stream_max_workers})는 "
                f"db_pool_max_size({self.db_pool_max_size}) 이하여야 한다 — "
                "스트림 동시 작업이 풀보다 많으면 연결 대기로 동시성이 풀 크기로 깎인다"
            )
        return self

    @property
    def resolved_consumer_name(self) -> str:
        """consumer_name 이 비어 있으면 hostname 으로 대체."""
        return self.consumer_name or socket.gethostname()

    @property
    def reclaim_consumer_name(self) -> str:
        """회수 구독자의 컨슈머 이름 — 새 메시지 구독자와 나눠 XINFO CONSUMERS 에서 구분한다.

        같은 그룹 안에서 이름이 갈리므로 어느 쪽이 몇 건을 들고 있는지 따로 보인다.
        두 워커(이력서·리포트)는 그룹이 달라 이름을 공유해도 부딪히지 않는다.
        """
        return f"{self.resolved_consumer_name}-reclaimer"

    @property
    def s3_endpoint(self) -> str | None:
        """빈 문자열은 None 으로(실서버 = 기본 S3 엔드포인트)."""
        return self.s3_endpoint_url or None


@lru_cache
def get_settings() -> Settings:
    """설정 싱글턴. 최초 호출 시 환경변수를 읽어 캐시한다."""
    return Settings()
