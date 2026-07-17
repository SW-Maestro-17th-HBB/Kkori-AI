"""FastStream 워커 진입점 (§2, §3).

`resume.parse.requested` 를 consumer group 으로 소비해 분석 파이프라인을 태우고,
단계마다 `resume.parse.status.changed` 를 발행한다.

실행: `faststream run src.main:app`

## Redis Stream 상호운용 (실 Redis 로 확인, 2026-07-17)
- Spring 은 각 필드를 **네이티브 스트림 필드**로 XADD 한다(`mapBacked`).
- **소비**: FastStream 이 네이티브 필드를 읽어 모델로 검증해준다 → 핸들러가 `ParseRequest` 를 바로 받는다.
- **발행**: FastStream 기본 발행은 `__data__` 바이너리 봉투로 감싸 Spring 이 못 읽는다.
  → 상태 발행은 redis 커넥션으로 **네이티브 필드**를 직접 XADD 한다(`publish_status`).
"""

from __future__ import annotations

from redis.asyncio import Redis

from faststream import FastStream
from faststream.redis import RedisBroker, StreamSub

from src.config import Settings, get_settings
from src.contract import AnalysisStatus, ParseRequest, StatusChanged

settings: Settings = get_settings()
broker = RedisBroker(settings.redis_url)
app = FastStream(broker)


async def publish_status(
    redis: Redis,
    resume_id: int,
    user_id: int,
    status: AnalysisStatus,
    message: str = "",
) -> None:
    """상태 이벤트를 Spring 이 읽는 **네이티브 필드** 형식으로 발행한다.

    FastStream 기본 발행(`__data__` 봉투)이 아니라 redis 커넥션으로 직접 XADD 한다.
    """
    payload = StatusChanged(
        resumeId=resume_id, userId=user_id, status=status, message=message
    ).encode()
    await redis.xadd(StatusChanged.STREAM_KEY, payload)


@broker.subscriber(
    stream=StreamSub(
        ParseRequest.STREAM_KEY,
        group=settings.consumer_group,
        consumer=settings.resolved_consumer_name,
        # 주의: StreamSub 에 min_idle_time 을 주면 '새 메시지 읽기'가 아니라 '방치된 PEL 회수' 모드가
        # 되어 갓 들어온 메시지를 못 읽는다(실측 확인). 회수(XAUTOCLAIM, §3)는 별도로 구현한다. TODO(§3).
    )
)
async def handle_parse_requested(request: ParseRequest) -> None:
    """분석 요청 처리 진입점 — 아직 뼈대.

    TODO(§4): 수신 직후 delivery count 확인 → 임계 초과 시 FAILED 후 ACK.
    TODO(§3.1): DB 상태로 재개 지점 결정(원자적 상태 CAS).
    TODO(§2): 파이프라인 단계(추출→구조화→청킹→임베딩→색인) 실행.

    현재는 수신·발행 경로 확인용으로 `PARSING` 만 발행한다(자리표시자).
    """
    await publish_status(
        broker._connection,
        request.resumeId,
        request.userId,
        AnalysisStatus.PARSING,
    )
