"""초기 질문 프롬프트 조립 테스트 — docs/prd/interview.md §2 검증 기준."""

from src.interview.prompts import (
    _INITIAL_QUESTION_POOL,
    INTERVIEWER_INSTRUCTIONS,
    _question_pool,
    initial_question_instructions,
)


def test_pool_has_two_questions_per_category():
    assert len(_INITIAL_QUESTION_POOL) == 8


def test_pool_substitutes_position():
    pool = _question_pool("백엔드")
    assert len(pool) == 8
    assert not any("{position}" in q for q in pool)
    assert "백엔드 쪽으로 진로를 정하게 된 이유가 있을까요?" in pool
    assert "백엔드 분야에서 스스로 어떤 강점이 있다고 생각하시는지 궁금합니다." in pool


def test_pool_excludes_position_sentences_when_absent():
    pool = _question_pool(None)
    assert len(pool) == 6
    assert not any("{position}" in q for q in pool)
    # 강점형은 중립 문장이 남는다
    assert "스스로 생각하시는 본인의 가장 큰 강점이 무엇인지 궁금합니다." in pool


def test_pool_is_career_neutral():
    # 재직 경력을 전제한 표현 금지 (신입 지원자 기준으로도 성립)
    assert not any("일해오" in q or "재직" in q or "경력" in q for q in _INITIAL_QUESTION_POOL)


def test_instructions_enforce_selection():
    text = initial_question_instructions(position="백엔드")
    assert "목록에 없는 새로운 질문을 만들지 마세요" in text
    assert "내용은 유지하세요" in text
    for question in _question_pool("백엔드"):
        assert question in text


def test_resume_context_is_selection_material_only():
    text = initial_question_instructions(position="백엔드", resume_context="기술: Java")
    assert "판단하는 데만 참고" in text
    assert "세부 경험을 언급하지는 마세요" in text
    assert "기술: Java" in text


def test_instructions_hold_without_optional_inputs():
    text = initial_question_instructions()
    assert "이력서 요약" not in text
    assert "백엔드" not in text
    assert "목록에 없는 새로운 질문을 만들지 마세요" in text


def test_system_prompt_constraints():
    # 페르소나·음성 제약·신입 전제는 시스템 프롬프트가 담당한다
    assert "면접관" in INTERVIEWER_INSTRUCTIONS
    assert "한국어" in INTERVIEWER_INSTRUCTIONS
    assert "구어체" in INTERVIEWER_INSTRUCTIONS
    assert "신입" in INTERVIEWER_INSTRUCTIONS
    assert "재직 경력을 전제한 표현" in INTERVIEWER_INSTRUCTIONS
