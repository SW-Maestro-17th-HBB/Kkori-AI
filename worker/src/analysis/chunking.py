"""청킹 — 엔티티 기반 + 초과 길이 분할 (PRD §2.5).

입력은 raw text 가 아니라 이미 의미 단위로 구조화된 `StructuredData` 다.
따라서 엔티티(프로젝트/경력/스킬 카테고리) 경계를 그대로 청크 경계로 삼는다.

- 1엔티티 = 1청크. profile 은 임베딩 제외(면접 질문 소스가 아님).
- 각 청크는 단독으로 읽혀도 뜻이 통하게 라벨을 앞에 붙인다(자기완결).
- 목표 크기를 넘는 엔티티만 문장 경계로 나누고, 각 조각에 헤더를 다시 붙인다
  + 조각 사이 1문장 겹침. 엔티티 사이에는 겹침이 없다.
- 빈 배열·빈 엔티티는 청크를 만들지 않는다(모두 비면 0청크 → EMBEDDED 종결은 §2.5).

크기 측정: 한국어 기준 대략 글자 수 ÷ 2 ≈ 토큰 수로 근사한다(§10 — 정밀 토큰 계산은
검색 품질 튜닝 때 도입). 문장 분할도 정규식 근사다.
"""

from __future__ import annotations

import re
from enum import Enum

from pydantic import BaseModel

from src.contract.structured_data import Experience, Project, Skill, StructuredData


class ChunkType(str, Enum):
    PROJECT = "project"
    EXPERIENCE = "experience"
    SKILL = "skill"


class Chunk(BaseModel):
    """임베딩·저장 단위. resume_chunks.content / metadata 로 들어간다."""

    content: str
    type: ChunkType
    source_index: int  # 원본 배열(projects[i] 등)에서의 위치
    label: str  # 엔티티 이름/카테고리 (예: 프로젝트명)
    chunk_version: int

    def metadata(self) -> dict:
        """resume_chunks.metadata(jsonb) 에 넣을 값."""
        return {
            "type": self.type.value,
            "source_index": self.source_index,
            "label": self.label,
            "chunk_version": self.chunk_version,
        }


# ---------------------------------------------------------------- 크기·문장 근사

_SENTENCE_END = re.compile(r"(?<=[.!?])\s+|\n+")


def approx_tokens(text: str) -> int:
    """한국어 대략 2글자 ≈ 1토큰으로 근사."""
    return len(text) // 2


def split_sentences(text: str) -> list[str]:
    """문장 경계(., !, ?, 줄바꿈) 정규식 근사 분할."""
    return [s.strip() for s in _SENTENCE_END.split(text) if s.strip()]


# ---------------------------------------------------------------- 엔티티 → content

def _project_content(p: Project) -> str:
    parts = [f"[프로젝트] {p.name}".strip()]
    if p.role:
        parts[0] += f" · 역할 {p.role}"
    if p.description:
        parts.append(p.description)
    if p.techStacks:
        parts.append(f"기술: {', '.join(p.techStacks)}")
    return "\n".join(parts)


def _experience_content(e: Experience) -> str:
    parts = [f"[경력] {e.title}".strip()]
    if e.description:
        parts.append(e.description)
    return "\n".join(parts)


def _skill_content(s: Skill) -> str:
    return f"[스킬] {s.category}: {', '.join(s.items)}"


def _is_empty_project(p: Project) -> bool:
    return not (p.name or p.role or p.description or p.techStacks)


def _is_empty_experience(e: Experience) -> bool:
    return not (e.title or e.description)


def _is_empty_skill(s: Skill) -> bool:
    return not s.items


# ---------------------------------------------------------------- 초과 길이 분할

def _split_oversized(
    header: str,
    body_sentences: list[str],
    tail: str,
    target_tokens: int,
    overlap_sentences: int,
) -> list[str]:
    """긴 본문을 문장 단위로 목표 크기 조각들로 묶는다.

    각 조각 앞에 header 를 다시 붙이고, 조각 사이에 overlap_sentences 개 문장을 겹친다.
    tail(기술 목록 등)은 마지막 조각에만 붙인다.
    """
    # 본문이 비면(설명 없는 엔티티가 header+tail 만으로 초과) 통짜 1청크로 폴백 —
    # 조각 0개가 되어 엔티티가 조용히 사라지는 유실 방지 (CodeRabbit 지적 반영)
    if not body_sentences:
        return ["\n".join(x for x in (header, tail) if x)]

    pieces: list[list[str]] = []
    current: list[str] = []
    budget = max(target_tokens - approx_tokens(header), 1)

    for sent in body_sentences:
        size = sum(approx_tokens(s) for s in current) + approx_tokens(sent)
        if current and size > budget:
            pieces.append(current)
            # 겹침: 직전 조각의 마지막 N문장을 다음 조각 앞에 복사
            current = current[-overlap_sentences:] if overlap_sentences else []
        current.append(sent)
    if current:
        pieces.append(current)

    out = []
    for i, piece in enumerate(pieces):
        text = "\n".join([header, " ".join(piece)])
        if tail and i == len(pieces) - 1:
            text += f"\n{tail}"
        out.append(text)
    return out


# ------------------------------------------------- 성과 단위 청킹 (§2.5 세분화 + §2.6)

def build_chunks(
    data: "StructuredData",
    enrichment,
    *,
    target_tokens: int = 512,
    overlap_sentences: int = 1,
    chunk_version: int = 3,
) -> tuple[list[Chunk], list[dict]]:
    """분할+풍부화 결과(ResumeEnrichment)로 청크와 metadata 병합분을 만든다.

    - 프로젝트: LLM 이 분리한 **성과 1개 = 청크 1개** (헤더+소개를 문맥으로 부착).
      성과 분할이 비어 있으면(짧은 설명·가짜 제공자) 기존 엔티티=청크로 폴백.
    - 경력·스킬: 기존 엔티티=청크 그대로 + 풍부화만 병합.
    - 반환: (청크 목록, 청크별 metadata 병합 dict 목록) — 저장 시 replace_chunks 에 전달.

    실측 근거(2026-07-21, 실제 이력서 A/B): 성과 단위가 1·2위 격차 +43%, 성과 문장 정밀 조준.
    """
    chunks: list[Chunk] = []
    extras: list[dict] = []

    def add(chunk: Chunk, extra: dict) -> None:
        chunks.append(chunk)
        extras.append(extra)

    for i, p in enumerate(data.projects):
        if _is_empty_project(p):
            continue
        analysis = enrichment.projects[i] if i < len(enrichment.projects) else None
        header = _project_content(Project(name=p.name, role=p.role))
        tail = f"기술: {', '.join(p.techStacks)}" if p.techStacks else ""

        if analysis is not None and analysis.achievements:
            # 성과 단위: 각 성과 = 청크 (헤더 + 소개 + 성과 문장 + 기술)
            intro = analysis.intro.strip()
            for j, ach in enumerate(analysis.achievements):
                parts = [header] + ([intro] if intro else []) + [ach.text]
                if tail:
                    parts.append(tail)
                add(
                    Chunk(
                        content="\n".join(parts), type=ChunkType.PROJECT,
                        source_index=i, label=p.name, chunk_version=chunk_version,
                    ),
                    {"achievement_index": j, "topics": ach.topics,
                     "relatedConcepts": ach.relatedConcepts,
                     "questionHints": ach.questionHints},
                )
        else:
            # 폴백: 분할이 없으면 기존 엔티티=청크 (초과 길이 분할 규칙 유지)
            for piece in _entity_pieces(header, p.description, tail,
                                        target_tokens, overlap_sentences):
                add(
                    Chunk(content=piece, type=ChunkType.PROJECT, source_index=i,
                          label=p.name, chunk_version=chunk_version),
                    {"topics": [], "relatedConcepts": [], "questionHints": []},
                )

    for i, e in enumerate(data.experiences):
        if _is_empty_experience(e):
            continue
        enr = enrichment.experiences[i] if i < len(enrichment.experiences) else None
        header = f"[경력] {e.title}".strip()
        for piece in _entity_pieces(header, e.description, "", target_tokens, overlap_sentences):
            add(
                Chunk(content=piece, type=ChunkType.EXPERIENCE, source_index=i,
                      label=e.title, chunk_version=chunk_version),
                _enrichment_extra(enr),
            )

    for i, sk in enumerate(data.skills):
        if _is_empty_skill(sk):
            continue
        enr = enrichment.skills[i] if i < len(enrichment.skills) else None
        add(
            Chunk(content=_skill_content(sk), type=ChunkType.SKILL, source_index=i,
                  label=sk.category, chunk_version=chunk_version),
            _enrichment_extra(enr),
        )

    return chunks, extras


def _enrichment_extra(enr) -> dict:
    if enr is None:
        return {"topics": [], "relatedConcepts": [], "questionHints": []}
    return {"topics": enr.topics, "relatedConcepts": enr.relatedConcepts,
            "questionHints": enr.questionHints}


def _entity_pieces(
    header: str, body: str, tail: str, target_tokens: int, overlap_sentences: int
) -> list[str]:
    """엔티티 1개를 content 조각(1개 또는 초과 길이 분할)으로 만든다 — 기존 §2.5 규칙."""
    whole = "\n".join(x for x in (header, body, tail) if x)
    if approx_tokens(whole) <= target_tokens:
        return [whole]
    return _split_oversized(header, split_sentences(body), tail, target_tokens, overlap_sentences)


# ---------------------------------------------------------------- 메인

def chunk_structured_data(
    data: StructuredData,
    *,
    target_tokens: int = 512,
    overlap_sentences: int = 1,
    chunk_version: int = 1,
) -> list[Chunk]:
    """StructuredData → 청크 목록. profile 은 제외한다."""
    chunks: list[Chunk] = []

    def add(content: str, ctype: ChunkType, idx: int, label: str) -> None:
        chunks.append(
            Chunk(
                content=content,
                type=ctype,
                source_index=idx,
                label=label,
                chunk_version=chunk_version,
            )
        )

    def add_entity(
        ctype: ChunkType, idx: int, label: str, header: str, body: str, tail: str
    ) -> None:
        whole = "\n".join(x for x in (header, body, tail) if x)
        if approx_tokens(whole) <= target_tokens:
            add(whole, ctype, idx, label)
            return
        for piece in _split_oversized(
            header, split_sentences(body), tail, target_tokens, overlap_sentences
        ):
            add(piece, ctype, idx, label)

    for i, p in enumerate(data.projects):
        if _is_empty_project(p):
            continue
        header = _project_content(Project(name=p.name, role=p.role))
        tail = f"기술: {', '.join(p.techStacks)}" if p.techStacks else ""
        add_entity(ChunkType.PROJECT, i, p.name, header, p.description, tail)

    for i, e in enumerate(data.experiences):
        if _is_empty_experience(e):
            continue
        header = f"[경력] {e.title}".strip()
        add_entity(ChunkType.EXPERIENCE, i, e.title, header, e.description, "")

    for i, s in enumerate(data.skills):
        if _is_empty_skill(s):
            continue
        add(_skill_content(s), ChunkType.SKILL, i, s.category)

    return chunks
