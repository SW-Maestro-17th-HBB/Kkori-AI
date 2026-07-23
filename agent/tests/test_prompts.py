"""초기 질문 프롬프트 조립 테스트 — docs/prd/interview.md §2 검증 기준."""

from src.interview.prompts import (
    _INITIAL_QUESTION_POOL,
    INTERVIEWER_INSTRUCTIONS,
    question_pool,
    position_label,
    selection_instructions,
)


def test_pool_has_two_questions_per_category():
    assert len(_INITIAL_QUESTION_POOL) == 8


def test_position_label_maps_enum_codes():
    # Spring 직무 enum 코드 → 발화용 표시명 (계약의 enum과 동기화)
    assert position_label("BACKEND") == "백엔드"
    assert position_label("FRONTEND") == "프론트엔드"
    assert position_label(" backend ") == "백엔드"
    # 한국어 표시명 자체도 허용 (픽스처·과도기 호환)
    assert position_label("백엔드") == "백엔드"


def test_position_label_rejects_unknown_values():
    # 발화에는 매핑된 표시명만 쓰인다 — 미등록 값은 직무 미지정 폴백 (enum 확장 시 매핑 동기화 필요)
    assert position_label(None) is None
    assert position_label("") is None
    assert position_label("AI") is None


def test_pool_substitutes_position():
    pool = question_pool("BACKEND")
    assert pool == question_pool("백엔드")
    assert len(pool) == 8
    assert not any("{position}" in q for q in pool)
    assert "백엔드 쪽으로 진로를 정하게 된 이유가 있을까요?" in pool
    assert "백엔드 분야에서 스스로 어떤 강점이 있다고 생각하시는지 궁금합니다." in pool


def test_pool_neutralizes_unknown_position():
    # 미등록 position은 중립 목록으로 폴백하고, 그 값이 목록·지시문에 유입되지 않는다
    assert question_pool("DEVOPS") == question_pool(None)
    assert "DEVOPS" not in selection_instructions(position="DEVOPS")


def test_pool_excludes_position_sentences_when_absent():
    pool = question_pool(None)
    assert len(pool) == 6
    assert not any("{position}" in q for q in pool)
    # 강점형은 중립 문장이 남는다
    assert "스스로 생각하시는 본인의 가장 큰 강점이 무엇인지 궁금합니다." in pool


def test_pool_is_career_neutral():
    # 재직 경력을 전제한 표현 금지 (신입 지원자 기준으로도 성립)
    assert not any("일해오" in q or "재직" in q or "경력" in q for q in _INITIAL_QUESTION_POOL)


def test_instructions_demand_number_only_output():
    text = selection_instructions(position="백엔드")
    assert "번호만 출력" in text
    assert "숫자 하나만" in text
    for number, question in enumerate(question_pool("백엔드"), start=1):
        assert f"{number}. {question}" in text


def test_resume_context_is_selection_material_only():
    text = selection_instructions(position="백엔드", resume_context="기술: Java")
    assert "판단하는 데만 참고" in text
    assert "기술: Java" in text


def test_instructions_hold_without_optional_inputs():
    text = selection_instructions()
    assert "이력서 요약" not in text
    assert "백엔드" not in text
    assert "번호만 출력" in text


def test_system_prompt_constraints():
    # 페르소나·음성 제약·신입 전제는 시스템 프롬프트가 담당한다
    assert "면접관" in INTERVIEWER_INSTRUCTIONS
    assert "한국어" in INTERVIEWER_INSTRUCTIONS
    assert "구어체" in INTERVIEWER_INSTRUCTIONS
    assert "신입" in INTERVIEWER_INSTRUCTIONS
    assert "재직 경력을 전제한 표현" in INTERVIEWER_INSTRUCTIONS
