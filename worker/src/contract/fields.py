"""스트림 필드맵 변환 — 계약 해석의 앞 단계라 공용 계층(contract)에 둔다.

Redis 가 주는 bytes 필드맵을 계약 모델의 decode() 가 기대하는 str 맵으로 바꾼다.
특정 도메인 계약에 묶이지 않으므로 이력서·리포트 양쪽이 여기서 가져다 쓴다
(도메인 간 공유는 contract·ai·config 계층으로 제한 — CLAUDE.md 작업 규칙).
"""

from __future__ import annotations


def decode_fields(fields: dict) -> dict[str, str]:
    """Redis 가 주는 bytes 필드맵을 계약 모델이 기대하는 str 맵으로."""
    return {
        (k.decode() if isinstance(k, bytes) else k): (
            v.decode() if isinstance(v, bytes) else v
        )
        for k, v in fields.items()
    }
