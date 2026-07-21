"""Spring 백엔드와의 계약 사본 (PRD §1) — 스트림 메시지 + StructuredData 스키마."""

from src.contract.messages import (
    AnalysisMode,
    AnalysisStatus,
    ParseRequest,
    StatusChanged,
)
from src.contract.structured_data import StructuredData

__all__ = [
    "AnalysisMode",
    "AnalysisStatus",
    "ParseRequest",
    "StatusChanged",
    "StructuredData",
]
