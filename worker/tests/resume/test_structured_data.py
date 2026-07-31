"""StructuredData 검증 방침 테스트 (PRD §1.4)."""

import pytest
from pydantic import ValidationError

from src.contract.structured_data import StructuredData


def test_빈_구조_허용():
    sd = StructuredData.model_validate({})
    assert sd.profile.name == ""
    assert sd.skills == []
    assert sd.projects == []


def test_모르는_필드_무시():
    sd = StructuredData.model_validate(
        {"profile": {"name": "홍길동", "unknown": 1}, "extra": 2}
    )
    assert sd.profile.name == "홍길동"


def test_정상_구조_읽기():
    data = {
        "profile": {"name": "홍길동", "email": "a@b.com"},
        "skills": [{"category": "언어", "items": ["Java", "Python"]}],
        "projects": [
            {"name": "P", "role": "백엔드", "description": "설명", "techStacks": ["Spring"]}
        ],
        "experiences": [{"title": "인턴", "description": "설명"}],
    }
    sd = StructuredData.model_validate(data)
    assert sd.skills[0].items == ["Java", "Python"]
    assert sd.projects[0].techStacks == ["Spring"]
    assert sd.experiences[0].title == "인턴"


def test_배열_내_null_요소_거부():
    with pytest.raises(ValidationError):
        StructuredData.model_validate(
            {"skills": [{"category": "언어", "items": ["Java", None]}]}
        )
    with pytest.raises(ValidationError):
        StructuredData.model_validate({"projects": [None]})
