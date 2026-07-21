"""구조화·임베딩 AI 제공자 — 인터페이스 + 가짜(fake)/Bedrock 구현.

파이프라인은 아래 Protocol(`Structurer`, `Embedder`)에만 의존하고,
설정(`ai_provider=fake|bedrock`)으로 구현을 교체한다.

- 가짜: 클라우드/과금 없이 파이프라인 로직을 결정적으로 테스트하기 위한 것.
- Bedrock: 실제 호출 — Claude(tool 강제로 StructuredData 형태 보장) + Cohere Embed v4
  (출력 차원 1024 지정, 색인/질의 비대칭). AWS 자격은 boto3 기본 체인
  (환경변수 `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` 또는 IAM Role).

관련: PRD §8(AI 제공자·리전·모델 ID), §2.5(청킹), §2.1(구조화).
"""

from __future__ import annotations

import hashlib
import json
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


# ---------------------------------------------------------------- Bedrock 구현

_STRUCTURE_PROMPT = """\
다음은 이력서 PDF 에서 추출한 원문 텍스트다. 이 텍스트에서 정보를 추출해
save_structured_data 도구로 저장하라.

규칙:
- 텍스트에 실제로 있는 정보만 추출한다. 추측하거나 지어내지 않는다.
- 없는 항목은 빈 값(빈 문자열·빈 배열)으로 둔다.
- 프로젝트/경력의 description 은 원문 표현을 최대한 보존해 요약 없이 담는다.
- 기술 스택은 원문 표기 그대로 (예: "Spring Boot" 를 "스프링"으로 바꾸지 않는다).

이력서 원문:
---
{text}
---
"""


class BedrockStructurer:
    """Claude(Bedrock) 구조화 — tool 강제 호출로 StructuredData 형태를 보장한다 (§8)."""

    def __init__(self, settings: Settings) -> None:
        from anthropic import AnthropicBedrock

        self._client = AnthropicBedrock(aws_region=settings.bedrock_region)
        self._model_id = settings.structuring_model_id

    def structure(self, text: str) -> StructuredData:
        tool = {
            "name": "save_structured_data",
            "description": "이력서에서 추출한 구조화 데이터를 저장한다.",
            "input_schema": StructuredData.model_json_schema(),
        }
        message = self._client.messages.create(
            model=self._model_id,
            max_tokens=4096,
            tools=[tool],
            tool_choice={"type": "tool", "name": "save_structured_data"},  # 형태 강제
            messages=[
                {"role": "user", "content": _STRUCTURE_PROMPT.format(text=text)}
            ],
        )
        for block in message.content:
            if block.type == "tool_use":
                return StructuredData.model_validate(block.input)
        raise ValueError("구조화 응답에 tool_use 블록이 없음")


class BedrockEmbedder:
    """Cohere Embed v4(Bedrock) — 출력 차원을 지정해 vector(dim) 스키마를 유지한다 (§8)."""

    _BATCH = 96  # Cohere 호출당 최대 텍스트 수

    def __init__(self, settings: Settings) -> None:
        import boto3

        self._client = boto3.client(
            "bedrock-runtime", region_name=settings.bedrock_region
        )
        self._model_id = settings.embedding_model_id
        self.dim = settings.embedding_dim

    def _invoke(self, texts: list[str], input_type: str) -> list[list[float]]:
        out: list[list[float]] = []
        for i in range(0, len(texts), self._BATCH):
            body = {
                "texts": texts[i : i + self._BATCH],
                "input_type": input_type,
                "embedding_types": ["float"],
                "output_dimension": self.dim,
            }
            response = self._client.invoke_model(
                modelId=self._model_id, body=json.dumps(body)
            )
            payload = json.loads(response["body"].read())
            embeddings = payload["embeddings"]
            if isinstance(embeddings, dict):  # embedding_types 지정 시 {"float": [...]}
                embeddings = embeddings["float"]
            out.extend(embeddings)
        return out

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return self._invoke(texts, "search_document") if texts else []

    def embed_query(self, text: str) -> list[float]:
        return self._invoke([text], "search_query")[0]


# ---------------------------------------------------------------- 팩토리

def build_structurer(settings: Settings) -> Structurer:
    if settings.ai_provider == "fake":
        return FakeStructurer()
    if settings.ai_provider == "bedrock":
        return BedrockStructurer(settings)
    raise ValueError(f"알 수 없는 ai_provider: {settings.ai_provider}")


def build_embedder(settings: Settings) -> Embedder:
    if settings.ai_provider == "fake":
        return FakeEmbedder(dim=settings.embedding_dim)
    if settings.ai_provider == "bedrock":
        return BedrockEmbedder(settings)
    raise ValueError(f"알 수 없는 ai_provider: {settings.ai_provider}")
