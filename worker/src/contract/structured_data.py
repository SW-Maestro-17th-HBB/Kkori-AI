"""LLM 구조화 결과 스키마 — `resumes.structured_data` (jsonb). PRD §1.4.

검증 방침(형태 엄격·내용 관대):
- 모르는 필드는 무시한다(extra="ignore").
- 필드 누락·빈 배열은 허용한다(기본값으로 채움).
- 단, 배열 안의 null 요소는 거부한다(청킹 단계에서 오류를 유발하므로).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class _Base(BaseModel):
    model_config = ConfigDict(extra="ignore")


class Profile(_Base):
    name: str = ""
    email: str = ""


class Skill(_Base):
    category: str = ""
    items: list[str] = Field(default_factory=list)


class Project(_Base):
    name: str = ""
    role: str = ""
    description: str = ""
    techStacks: list[str] = Field(default_factory=list)


class Experience(_Base):
    title: str = ""
    description: str = ""


class StructuredData(_Base):
    profile: Profile = Field(default_factory=Profile)
    skills: list[Skill] = Field(default_factory=list)
    projects: list[Project] = Field(default_factory=list)
    experiences: list[Experience] = Field(default_factory=list)
