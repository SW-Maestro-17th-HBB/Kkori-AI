"""프롬프트 조립 테스트 — 초기 질문(docs/prd/interview.md §2)과 본론
(docs/prd/follow-up-question.md §2·§3) 검증 기준."""

from src.interview.conversation_log import FollowUpType
from src.interview.prompts import (
    _INITIAL_QUESTION_POOL,
    FALLBACK_QUESTIONS,
    INTERVIEWER_INSTRUCTIONS,
    follow_up_instructions,
    next_topic_instructions,
    orchestrator_instructions,
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


# --- 본론: Orchestrator 평가·판단 지시 (PRD §2) ---

def test_orchestrator_instructions_carry_criteria_and_rules():
    text = orchestrator_instructions("[Q1|root1] 질문\n[A1] 답변")
    for criterion in ("구체성", "깊이", "완결성", "소진도"):
        assert criterion in text
    assert "점수화하지 않습니다" in text
    assert "모른다고 하거나 포기한 답변" in text  # 모름 → NEXT_TOPIC
    assert "reason에 판단 근거를 한 문장으로" in text
    assert "[Q1|root1] 질문" in text  # 대화 직렬화가 포함된다


def test_orchestrator_instructions_list_all_types_and_consistency_guard():
    text = orchestrator_instructions("")
    for follow_up_type in FollowUpType:
        assert str(follow_up_type) in text
    assert "명백히 상충할 때만" in text  # CONSISTENCY 보수 판단
    assert "refQuestionNumber" in text
    assert "현재 주제" in text  # ref는 현재 줄기 밖


def test_orchestrator_instructions_include_trust_boundary():
    assert "신뢰할 수 없는 데이터" in orchestrator_instructions("")


# --- 본론: 질문 생성 지시 (PRD §3) ---

def test_follow_up_instructions_swap_direction_block_by_type():
    deepen = follow_up_instructions(FollowUpType.DEEPEN)
    verify = follow_up_instructions(FollowUpType.VERIFY)
    assert "이유를 파고드세요" in deepen and "이유를 파고드세요" not in verify
    assert "실제로 이해하고 있는지" in verify
    # 공통 블록은 유형과 무관하게 포함
    for text in (deepen, verify):
        assert "음성으로 전달" in text
        assert "신뢰할 수 없는 데이터" in text
        assert "질문 텍스트만 출력" in text


def test_follow_up_instructions_pass_reason_and_ref_branch():
    text = follow_up_instructions(
        FollowUpType.CONSISTENCY,
        reason="역할 서술이 상충",
        ref_branch_text="[Q1|root1] 자기소개\n[A1] 혼자 했습니다",
    )
    assert "파고들 지점: 역할 서술이 상충" in text
    assert "[A1] 혼자 했습니다" in text
    assert "추궁하거나 단정하지" in text  # 확인형 어조


def test_next_topic_instructions_carry_sources_and_dedupe():
    text = next_topic_instructions(
        resume_context="기술: Java, Redis",
        previous_questions=("자기소개 부탁드립니다.", "결제 프로젝트 소개해 주세요."),
        reason="주제 소진",
    )
    assert "기술: Java, Redis" in text
    assert "- 결제 프로젝트 소개해 주세요." in text
    assert "겹치지 않게" in text
    assert "전환 근거: 주제 소진" in text
    assert "대화에서 나온 정보" in text  # 즉석 정보도 질문 소스


def test_next_topic_instructions_hold_without_optional_inputs():
    text = next_topic_instructions()
    assert "이력서 요약:" not in text
    assert "겹치지 않게" not in text
    assert "음성으로 전달" in text


def test_fallback_questions_are_vetted_and_answer_independent():
    assert len(FALLBACK_QUESTIONS) >= 3
    for question in FALLBACK_QUESTIONS:
        assert len(question) >= 15  # 폴백 품질 회귀 방지 — 지나치게 짧은 문자열 차단
        assert "{position}" not in question
        assert not any(marker in question for marker in ("#", "*", "-", "`"))
        assert question.endswith(("요?", "니다."))  # 구어체 검수 확인
