"""리포트 파이프라인의 Redis Stream 메시지·jsonb 계약.

이 파일은 백엔드 계약의 자기완결 사본이다(변경 권한은 백엔드에 있음 —
`Kkori-Backend` docs/requirements/report/report.md §1 인터페이스 요구사항).
필드명은 전송 계약의 키와 1:1로 맞추기 위해 camelCase 를 그대로 쓴다.

주의: 세션 도메인이 발행하는 두 메시지(GenerationRequested/AudioAnalysisRequested)의 필드와
스트림 키 이름은 **면접 도메인과 합의 전 잠정**이다 — 합의 확정 시 백엔드 계약
record와 함께 이 파일·골든 샘플을 한 커밋으로 갱신한다.
"""

from __future__ import annotations

from enum import Enum
from typing import ClassVar, Mapping

from pydantic import BaseModel


class ReportStatus(str, Enum):
    """리포트 생성 상태 4종 (백엔드 PRD §Overview). 뒤 두 개가 종결 상태."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class WeaknessTagCount(BaseModel):
    """REPORTS.weakness_tag_summary(jsonb) 요소 — 태그 빈도. 스키마 원천은 백엔드."""

    tag: str
    count: int


class ImprovementTask(BaseModel):
    """REPORT_FEEDBACKS.improvement_tasks(jsonb) 요소 — 개선 과제. 스키마 원천은 백엔드."""

    title: str
    description: str


class ReportGenerationRequested(BaseModel):
    """`report.generation.requested` — 워커가 소비하는 리포트 생성 요청.

    세션 도메인이 실전(30분) 면접의 정상 종료 시에만 발행한다("리포트가 필요하다"는
    요청 — 이력서의 resume.parse.requested 와 같은 요청형 계약).
    at-least-once 중복 전달에 대비해 소비 처리는 sessionId 기준 멱등이어야 한다.
    """

    STREAM_KEY: ClassVar[str] = "report.generation.requested"

    sessionId: int
    userId: int

    @classmethod
    def decode(cls, fields: Mapping[str, str]) -> "ReportGenerationRequested":
        return cls.model_validate(dict(fields))

    def encode(self) -> dict[str, str]:
        return {"sessionId": str(self.sessionId), "userId": str(self.userId)}


class AudioAnalysisRequested(BaseModel):
    """`report.audio.analysis.requested` — 음성 분석(2단계) 요청.

    세션 도메인이 녹음 파일을 S3 호환 저장소에 올린 뒤 발행한다.
    처리 실패가 반복돼도 리포트를 FAILED 로 만들지 않는다 — 포기 ACK 후
    유예 완성 경로(delivery null)로 넘긴다. FAILED 는 텍스트 경로 실패에 한정.
    """

    STREAM_KEY: ClassVar[str] = "report.audio.analysis.requested"

    sessionId: int
    bucket: str
    objectKey: str

    @classmethod
    def decode(cls, fields: Mapping[str, str]) -> "AudioAnalysisRequested":
        return cls.model_validate(dict(fields))

    def encode(self) -> dict[str, str]:
        return {
            "sessionId": str(self.sessionId),
            "bucket": self.bucket,
            "objectKey": self.objectKey,
        }


class RegenerateRequested(BaseModel):
    """`report.regenerate.requested` — Spring 재생성 API가 발행, 워커가 소비.

    워커는 텍스트 분석만 다시 수행한다 — 음성 산출물(delivery·audio_analyzed_at)은
    Spring 이 보존한다(녹음은 분석 후 삭제되어 재분석 불가). 이미 완결(COMPLETED)된
    리포트에 대한 중복 전달은 무해하게 스킵 ACK 한다.
    """

    STREAM_KEY: ClassVar[str] = "report.regenerate.requested"

    reportId: int
    sessionId: int
    userId: int

    @classmethod
    def decode(cls, fields: Mapping[str, str]) -> "RegenerateRequested":
        return cls.model_validate(dict(fields))

    def encode(self) -> dict[str, str]:
        return {
            "reportId": str(self.reportId),
            "sessionId": str(self.sessionId),
            "userId": str(self.userId),
        }


class ReportStatusChanged(BaseModel):
    """`report.status.changed` — 워커가 상태 전이마다 발행, Spring이 소비(SSE 중계).

    PENDING 은 발행하지 않는다(로우 생성 직후의 짧은 초기 상태 — 백엔드 PRD §5).
    """

    STREAM_KEY: ClassVar[str] = "report.status.changed"

    reportId: int
    userId: int
    status: ReportStatus
    # status로 유도할 수 없는 정보(실패 사유 등). 계약상 null 은 "" 로 직렬화한다.
    message: str = ""

    @classmethod
    def decode(cls, fields: Mapping[str, str]) -> "ReportStatusChanged":
        return cls.model_validate(dict(fields))

    def encode(self) -> dict[str, str]:
        return {
            "reportId": str(self.reportId),
            "userId": str(self.userId),
            "status": self.status.value,
            "message": self.message or "",
        }
