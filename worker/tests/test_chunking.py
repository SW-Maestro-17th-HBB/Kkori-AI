"""청킹(§2.5) 테스트 — 엔티티 기반 + 초과 길이 분할."""

from src.analysis.chunking import ChunkType, approx_tokens, chunk_structured_data, split_sentences
from src.contract.structured_data import Experience, Profile, Project, Skill, StructuredData


def _sample() -> StructuredData:
    return StructuredData(
        profile=Profile(name="홍길동", email="a@b.com"),
        skills=[Skill(category="백엔드", items=["Java", "Spring"])],
        projects=[
            Project(name="주문 시스템", role="백엔드", description="Redis Stream 도입.", techStacks=["Redis"]),
            Project(name="검색 개선", role="", description="pgvector 적용.", techStacks=[]),
        ],
        experiences=[Experience(title="인턴", description="API 개발.")],
    )


def test_엔티티당_1청크_profile_제외():
    chunks = chunk_structured_data(_sample())
    # 프로젝트2 + 경력1 + 스킬1 = 4, profile 은 없음
    assert len(chunks) == 4
    assert {c.type for c in chunks} == {ChunkType.PROJECT, ChunkType.EXPERIENCE, ChunkType.SKILL}
    assert all("홍길동" not in c.content for c in chunks)


def test_content_자기완결_라벨():
    chunks = chunk_structured_data(_sample())
    proj = [c for c in chunks if c.type is ChunkType.PROJECT][0]
    assert proj.content.startswith("[프로젝트] 주문 시스템 · 역할 백엔드")
    assert "Redis Stream 도입." in proj.content
    assert "기술: Redis" in proj.content
    skill = [c for c in chunks if c.type is ChunkType.SKILL][0]
    assert skill.content == "[스킬] 백엔드: Java, Spring"


def test_metadata_구성():
    chunks = chunk_structured_data(_sample(), chunk_version=3)
    proj0 = [c for c in chunks if c.type is ChunkType.PROJECT and c.source_index == 0][0]
    assert proj0.metadata() == {
        "type": "project",
        "source_index": 0,
        "label": "주문 시스템",
        "chunk_version": 3,
    }


def test_빈_이력서는_0청크():
    assert chunk_structured_data(StructuredData()) == []


def test_빈_엔티티는_건너뜀():
    data = StructuredData(
        projects=[Project(), Project(name="유효")],
        skills=[Skill(category="빈", items=[])],
        experiences=[Experience()],
    )
    chunks = chunk_structured_data(data)
    assert len(chunks) == 1
    assert chunks[0].label == "유효"


def test_초과_길이_분할_헤더반복_겹침():
    long_desc = " ".join(f"{i}번째 문장입니다." for i in range(1, 101))  # 충분히 긴 본문
    data = StructuredData(
        projects=[Project(name="대형", role="리드", description=long_desc, techStacks=["Go"])]
    )
    chunks = chunk_structured_data(data, target_tokens=100, overlap_sentences=1)

    assert len(chunks) > 1  # 분할됨
    # 모든 조각에 헤더 반복
    assert all(c.content.startswith("[프로젝트] 대형 · 역할 리드") for c in chunks)
    # 같은 엔티티 → 같은 source_index/label
    assert {c.source_index for c in chunks} == {0}
    assert {c.label for c in chunks} == {"대형"}
    # 기술 목록은 마지막 조각에만
    assert chunks[-1].content.endswith("기술: Go")
    assert all("기술: Go" not in c.content for c in chunks[:-1])
    # 겹침: 앞 조각의 마지막 문장이 다음 조각에 다시 등장
    for prev, nxt in zip(chunks, chunks[1:]):
        last_sentence = split_sentences(prev.content.split("\n")[1])[-1]
        assert last_sentence in nxt.content
    # 각 조각이 목표 크기를 크게 넘지 않음 (겹침·헤더 감안해 2배 이내)
    assert all(approx_tokens(c.content) <= 200 for c in chunks)


def test_짧은_엔티티는_분할되지_않음():
    chunks = chunk_structured_data(_sample(), target_tokens=512)
    assert len([c for c in chunks if c.source_index == 0 and c.type is ChunkType.PROJECT]) == 1
