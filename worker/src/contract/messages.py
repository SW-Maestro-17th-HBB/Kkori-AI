"""Spring 백엔드 ↔ 워커 사이 Redis 메시지 계약 (요청은 Stream, 상태는 Pub/Sub).

이 파일은 백엔드 계약 record의 자기완결 사본이다(변경 권한은 백엔드에 있음).
값은 전부 문자열이다 — 요청은 Redis Stream 필드로 오고, 상태는 같은 문자열맵을 JSON 으로
Pub/Sub 채널에 발행한다. 아래 모델이 문자열 필드맵 ↔ 파이썬 모델 변환을 책임진다.
필드명은 전송 계약의 키와 1:1로 맞추기 위해 camelCase 를 그대로 쓴다(파이썬 관례보다 계약 일치를 우선).

계약 상세: worker/docs/requirements/resume-analysis/pipeline.md §1
"""

from __future__ import annotations

from enum import Enum
from typing import ClassVar, Mapping

from pydantic import BaseModel


class AnalysisMode(str, Enum):
    """분석 모드 (§1.1). FULL=전체 재수행, REINDEX=재색인만."""

    FULL = "FULL"
    REINDEX = "REINDEX"


class AnalysisStatus(str, Enum):
    """분석 상태 8종 (§1.3). 마지막 두 개(EMBEDDED/FAILED)가 종결 상태."""

    UPLOADED = "UPLOADED"
    PARSING = "PARSING"
    TEXT_EXTRACTING = "TEXT_EXTRACTING"
    STRUCTURING = "STRUCTURING"
    PARSED = "PARSED"
    EMBEDDING = "EMBEDDING"
    EMBEDDED = "EMBEDDED"
    FAILED = "FAILED"


class ParseRequest(BaseModel):
    """`resume.parse.requested` — 워커가 소비하는 분석 요청 (§1.1)."""

    STREAM_KEY: ClassVar[str] = "resume.parse.requested"

    resumeId: int
    userId: int
    bucket: str
    objectKey: str
    mode: AnalysisMode

    @classmethod
    def decode(cls, fields: Mapping[str, str]) -> "ParseRequest":
        """Redis 문자열 필드맵 → 모델. 타입·mode 값이 어긋나면 검증 오류를 낸다."""
        return cls.model_validate(dict(fields))

    def encode(self) -> dict[str, str]:
        """모델 → Redis 문자열 필드맵 (왕복 검증·재발행용). 모든 값은 문자열."""
        return {
            "resumeId": str(self.resumeId),
            "userId": str(self.userId),
            "bucket": self.bucket,
            "objectKey": self.objectKey,
            "mode": self.mode.value,
        }


class StatusChanged(BaseModel):
    """`resume.parse.status.changed` — 워커가 단계마다 발행하는 상태 (§1.2).

    Pub/Sub 채널에 JSON 으로 발행하고, Spring 전 인스턴스가 구독한다(SSE 중계).
    스트림이 아닌 이유: Consumer Group 으로 읽으면 메시지가 인스턴스에 나뉘어
    SSE 연결이 없는 쪽이 받은 몫은 버려지기 때문 (HBB1-332).
    """

    CHANNEL: ClassVar[str] = "resume.parse.status.changed"

    resumeId: int
    userId: int
    status: AnalysisStatus
    # status로 유도할 수 없는 정보(실패 사유 등). 계약상 null 은 "" 로 직렬화한다.
    message: str = ""

    @classmethod
    def decode(cls, fields: Mapping[str, str]) -> "StatusChanged":
        """문자열 필드맵(JSON 페이로드) → 모델 (주로 테스트·검증용). 없는 message 는 "" 로 취급."""
        return cls.model_validate(dict(fields))

    def encode(self) -> dict[str, str]:
        """모델 → 문자열 필드맵(JSON 으로 직렬화해 발행). message 는 null/빈값 → "" 규칙을 적용."""
        return {
            "resumeId": str(self.resumeId),
            "userId": str(self.userId),
            "status": self.status.value,
            "message": self.message or "",
        }
