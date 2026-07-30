"""Bedrock 실 호출 통합 테스트 — 명시적으로 켰을 때만 실행한다 (과금 방지).

실행 방법 (worker/.env 에 AWS 자격이 있어야 함):
    KKORI_LIVE_BEDROCK=1 pytest worker/tests/test_bedrock_live.py -v

검증 대상 (가짜 제공자로는 확인 불가한 것들):
- Bedrock 요청/응답 형식이 실제로 맞는가 (모델 ID·리전·본문 스키마)
- Claude 가 한국어 이력서를 실제로 구조화하는가
- Embed v4 가 지정 차원(1024)의 벡터를 주는가 + 의미 유사도가 실제로 작동하는가
"""

import math
import os
from pathlib import Path

import pytest

from src.config import Settings

LIVE = os.environ.get("KKORI_LIVE_BEDROCK") == "1"
ENV_FILE = Path(__file__).parent.parent.parent / ".env"  # worker/.env

pytestmark = pytest.mark.skipif(
    not LIVE, reason="실 Bedrock 호출 테스트 — KKORI_LIVE_BEDROCK=1 로만 실행(과금)"
)

SAMPLE_RESUME = """
홍길동
이메일: hong@example.com

[프로젝트]
주문 처리 시스템 (백엔드 개발)
- Redis Stream 을 도입해 주문 이벤트를 비동기 처리, 처리량을 3배 개선했습니다.
- 사용 기술: Java, Spring Boot, Redis

[경력]
OO스타트업 백엔드 인턴 (6개월)
- REST API 설계와 운영을 담당했습니다.

[기술]
Java, Spring Boot, Redis, PostgreSQL, Docker
"""


def _settings() -> Settings:
    # worker/.env 의 AWS 자격·모델 ID 를 읽고, 제공자만 bedrock 으로 강제
    s = Settings(_env_file=ENV_FILE)
    # pydantic-settings 는 AWS_* 를 모델 필드로 안 읽으므로 환경변수로 올린다 (boto3 기본 체인용)
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        if key not in os.environ and ENV_FILE.exists():
            for line in ENV_FILE.read_text().splitlines():
                if line.startswith(f"{key}="):
                    os.environ[key] = line.split("=", 1)[1].strip()
    return Settings(_env_file=ENV_FILE, ai_provider="bedrock")


def test_claude_구조화_실호출():
    from src.ai.providers import BedrockStructurer

    data = BedrockStructurer(_settings()).structure(SAMPLE_RESUME)

    assert data.profile.name == "홍길동"
    assert len(data.projects) >= 1
    project = data.projects[0]
    assert "주문" in project.name
    assert any("Redis" in t for t in project.techStacks)
    assert len(data.experiences) >= 1
    print(f"\n구조화 결과: {data.model_dump()}")


def test_embed_v4_차원과_의미유사도_실호출():
    from src.ai.providers import BedrockEmbedder

    embedder = BedrockEmbedder(_settings())
    docs = embedder.embed_documents(
        ["Redis Stream 으로 비동기 주문 처리를 구현했다", "오늘 점심은 김치찌개였다"]
    )
    query = embedder.embed_query("레디스 스트림 경험")

    # 형식: 지정 차원(1024) 벡터
    assert len(docs) == 2 and all(len(v) == 1024 for v in docs)
    assert len(query) == 1024

    # 의미: 질의는 김치찌개보다 Redis 문서에 가까워야 한다 (가짜 임베딩은 불가능한 검증)
    def cos(a, b):
        dot = sum(x * y for x, y in zip(a, b))
        return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(x * x for x in b)))

    sim_redis, sim_lunch = cos(query, docs[0]), cos(query, docs[1])
    print(f"\n유사도 — Redis 문서: {sim_redis:.3f}, 점심 문서: {sim_lunch:.3f}")
    assert sim_redis > sim_lunch


# ---------------------------------------------------------------- 실제 PDF 로 돌리기

RESUME_PDF = os.environ.get("KKORI_LIVE_RESUME_PDF", "")


@pytest.mark.skipif(
    not RESUME_PDF, reason="실제 이력서 PDF 테스트 — KKORI_LIVE_RESUME_PDF=<pdf 경로> 로 실행"
)
def test_실제_이력서PDF_추출_구조화_청킹():
    """아무 이력서 PDF 나 넣어 추출→구조화→청킹을 실제로 돌려본다 (품질 눈검사용).

    실행 예:
        KKORI_LIVE_BEDROCK=1 KKORI_LIVE_RESUME_PDF="/path/to/이력서.pdf" \
            pytest worker/tests/test_bedrock_live.py::test_실제_이력서PDF_추출_구조화_청킹 -s
    """
    import json

    from src.ai.providers import BedrockStructurer
    from src.analysis.chunking import approx_tokens, chunk_structured_data
    from src.analysis.extraction import extract_text, is_empty_text

    pdf = Path(RESUME_PDF).read_bytes()
    text = extract_text(pdf)
    assert not is_empty_text(text), "빈 추출 — 이미지 스캔 PDF 인지 확인"
    print(f"\n[추출] {len(pdf):,} bytes PDF → 텍스트 {len(text):,}자")

    data = BedrockStructurer(_settings()).structure(text)
    print("\n[구조화 결과]")
    print(json.dumps(data.model_dump(), ensure_ascii=False, indent=2))
    assert data.profile.name, "이름을 못 뽑음"
    assert data.projects or data.experiences, "프로젝트/경력을 하나도 못 뽑음"

    chunks = chunk_structured_data(data)
    print(f"\n[청킹] {len(chunks)}개")
    for c in chunks:
        print(f"  [{c.type.value}] {c.label!r} (~{approx_tokens(c.content)} 토큰)")
    assert chunks, "청크가 0개"
