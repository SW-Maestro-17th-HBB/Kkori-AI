"""가짜 AI 제공자 테스트."""

import pytest

from src.ai import (
    Embedder,
    FakeEmbedder,
    FakeStructurer,
    Structurer,
    build_embedder,
    build_structurer,
)
from src.config import Settings
from src.structured_data import Profile, StructuredData


def test_가짜_구조화기_정해진값_반환():
    sd = StructuredData(profile=Profile(name="홍길동"))
    structurer = FakeStructurer(sd)
    assert structurer.structure("아무 텍스트나").profile.name == "홍길동"


def test_가짜_구조화기_기본은_빈_구조():
    assert FakeStructurer().structure("x").projects == []


def test_가짜_임베딩_차원과_결정성():
    embedder = FakeEmbedder(dim=1024)
    v1 = embedder.embed_query("파이썬")
    v2 = embedder.embed_query("파이썬")
    v3 = embedder.embed_query("자바")
    assert len(v1) == 1024
    assert v1 == v2  # 같은 텍스트 → 같은 벡터 (결정적)
    assert v1 != v3  # 다른 텍스트 → 다른 벡터
    assert all(0.0 <= x <= 1.0 for x in v1)


def test_embed_documents_순서와_개수_보존():
    embedder = FakeEmbedder(dim=8)
    vecs = embedder.embed_documents(["a", "b", "c"])
    assert len(vecs) == 3
    assert all(len(v) == 8 for v in vecs)
    # 개별 embed_query 와 동일해야 (문서/질의 동일 텍스트 → 이 가짜는 같은 벡터)
    assert vecs[0] == embedder.embed_query("a")


def test_팩토리_fake_선택():
    s = Settings(ai_provider="fake")
    assert isinstance(build_structurer(s), Structurer)
    assert isinstance(build_embedder(s), Embedder)


def test_팩토리_bedrock은_아직_미구현():
    s = Settings(ai_provider="bedrock")
    with pytest.raises(NotImplementedError):
        build_structurer(s)
    with pytest.raises(NotImplementedError):
        build_embedder(s)
