"""Spring 백엔드와의 계약 사본 (PRD §1) — 스트림 메시지 + StructuredData 스키마."""

from src.contract.messages import (
    AnalysisMode,
    AnalysisStatus,
    ParseRequest,
    StatusChanged,
)
from src.contract.report import (
    AudioAnalysisRequested,
    ImprovementTask,
    RegenerateRequested,
    ReportStatus,
    ReportStatusChanged,
    ReportGenerationRequested,
    WeaknessTagCount,
)
from src.contract.structured_data import StructuredData
from src.contract.transcript import (
    QuestionAnswer,
    QuestionType,
    Speaker,
    Utterance,
    group_utterances,
)

__all__ = [
    "AnalysisMode",
    "AnalysisStatus",
    "AudioAnalysisRequested",
    "ImprovementTask",
    "ParseRequest",
    "QuestionAnswer",
    "QuestionType",
    "RegenerateRequested",
    "ReportStatus",
    "ReportStatusChanged",
    "ReportGenerationRequested",
    "Speaker",
    "StatusChanged",
    "StructuredData",
    "Utterance",
    "WeaknessTagCount",
    "group_utterances",
]
