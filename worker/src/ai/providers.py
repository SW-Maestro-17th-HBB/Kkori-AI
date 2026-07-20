"""구조화·임베딩 AI 제공자 — 인터페이스 + 가짜(fake) 구현.

파이프라인은 아래 Protocol(`Structurer`, `Embedder`)에만 의존한다.
실제 Bedrock 구현은 클라우드 준비 후 추가하고, 설정(`ai_provider`)으로 가짜/실제를 교체한다.

- 가짜 구조화기: 미리 정한 `StructuredData` 를 반환(테스트에서 주입).
- 가짜 임베딩기: 텍스트 해시 기반 **결정적 더미 벡터**(dim 차원). 값의 의미는 없고, 파이프라인
  로직(청킹·저장·재개)을 클라우드/과금 없이 결정적으로 테스트하기 위한 것.

관련: PRD §8(AI 제공자 추상화), §2.5(청킹), §2.1(구조화).
"""

from __future__ import annotations

import hashlib
from typing import Protocol, runtime_checkable

from src.config import Settings
from src.contract.structured_data import StructuredData


@runtime_checkable
class Structurer(Protocol):
    """이력서 원문 텍스트 → StructuredData (구조화 LLM)."""

    def structure(self, text: str) -> StructuredData: ...


@runtime_checkable
class Embedder(Protocol):
    """텍스트 → 임베딩 벡터. 색인/질의를 비대칭으로 구분한다(Cohere)."""

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """색인용 임베딩(search_document). 입력 순서와 동일 순서로 반환."""
        ...

    def embed_query(self, text: str) -> list[float]:
        """질의용 임베딩(search_query). 주로 검색(agent) 측에서 사용."""
        ...


class FakeStructurer:
    """정해진 StructuredData 를 반환하는 가짜. 반환값을 주입해 다양한 케이스를 재현한다."""

    def __init__(self, result: StructuredData | None = None) -> None:
        self._result = result if result is not None else StructuredData()

    def structure(self, text: str) -> StructuredData:
        return self._result


class FakeEmbedder:
    """텍스트 해시 기반 결정적 더미 벡터를 만드는 가짜.

    같은 텍스트 → 항상 같은 벡터, 다른 텍스트 → 다른 벡터. 값은 [0, 1] 범위.
    임베딩 '품질'은 없음 — 파이프라인 로직 검증 전용.
    """

    def __init__(self, dim: int = 1024) -> None:
        self.dim = dim

    def _vector(self, text: str) -> list[float]:
        out: list[float] = []
        i = 0
        while len(out) < self.dim:
            digest = hashlib.sha256(f"{text}:{i}".encode("utf-8")).digest()
            out.extend(b / 255.0 for b in digest)
            i += 1
        return out[: self.dim]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._vector(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._vector(text)


def build_structurer(settings: Settings) -> Structurer:
    if settings.ai_provider == "fake":
        return FakeStructurer()
    raise NotImplementedError(
        "Bedrock 구조화 제공자는 클라우드 준비 후 구현한다 (PRD §8)."
    )


def build_embedder(settings: Settings) -> Embedder:
    if settings.ai_provider == "fake":
        return FakeEmbedder(dim=settings.embedding_dim)
    raise NotImplementedError(
        "Bedrock 임베딩 제공자는 클라우드 준비 후 구현한다 (PRD §8)."
    )
