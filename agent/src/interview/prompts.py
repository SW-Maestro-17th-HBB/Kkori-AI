"""면접관 프롬프트 — 페르소나·음성 제약·초기 질문 선택 지시. docs/prd/interview.md §2."""

from __future__ import annotations

# 시스템 프롬프트는 역할·지시만 담는다. 지원 직무·이력서 요약은 초기 질문 지시에서 별도 주입.
INTERVIEWER_INSTRUCTIONS = (
    "당신은 AI 모의 면접관 '꼬리'입니다. 진지하되 적대적이지 않은 기술 면접관으로서 "
    "항상 한국어로 대화합니다. 모든 발화는 음성으로 전달됩니다. "
    "한두 문장으로 간결하게 말하고, 마크다운이나 리스트나 이모지나 특수문자 없이 "
    "말하듯 자연스러운 구어체를 사용하세요. "
    "지원자는 대부분 신입 또는 주니어입니다. 재직 경력을 전제한 표현을 쓰지 마세요."
)

# 초기 질문 발화는 코드가 조립한다 — 고정 인사말 + 선택된 목록 원문 (LLM 자유 생성 없음)
INITIAL_GREETING = "안녕하세요, 만나서 반갑습니다. 편하게 답변해 주시면 됩니다."

# 초기 질문 목록 — 유형별 2개(자기소개/지원동기/경험개괄/강점). LLM은 이 검수된 목록에서
# 번호로 고르기만 한다(few-shot 생성은 예시를 어중간하게 섞은 질문이 나와 폐기).
# {position} 문장은 직무가 있을 때만 목록에 포함하며 치환은 코드가 처리한다.
_INITIAL_QUESTION_POOL = (
    # 자기소개형
    "간단하게 자기소개 부탁드립니다.",
    "어떤 분인지 궁금한데, 본인 소개를 짧게 해주시겠어요?",
    # 지원동기형
    "이 직무에 지원하게 된 계기가 궁금합니다.",
    "{position} 쪽으로 진로를 정하게 된 이유가 있을까요?",
    # 경험개괄형
    "지금까지 했던 프로젝트 중에 가장 기억에 남는 경험을 하나 소개해 주시겠어요?",
    "최근에 가장 몰입해서 진행했던 작업이 있다면 어떤 것이었나요?",
    # 강점형
    "{position} 분야에서 스스로 어떤 강점이 있다고 생각하시는지 궁금합니다.",
    "스스로 생각하시는 본인의 가장 큰 강점이 무엇인지 궁금합니다.",
)


# 지원 직무 표시명 — Spring 직무 enum 코드를 발화 가능한 한국어로 변환한다.
# 세션 생성 계약의 enum과 동기화할 것 (현재 2종). 발화에는 이 표시명만 쓰이므로
# 미등록 값은 직무 미지정으로 폴백된다(임의 문자열이 발화로 유입되지 않음).
_POSITION_LABELS = {
    "BACKEND": "백엔드",
    "FRONTEND": "프론트엔드",
}


def position_label(position: str | None) -> str | None:
    """position 값(enum 코드 또는 표시명)을 발화용 표시명으로 변환한다. 미등록 값은 None."""
    if not position:
        return None
    key = position.strip()
    label = _POSITION_LABELS.get(key.upper())
    if label:
        return label
    # 한국어 표시명 자체도 허용 — 픽스처·과도기 호환
    return key if key in _POSITION_LABELS.values() else None


def question_pool(position: str | None) -> tuple[str, ...]:
    """직무 치환이 끝난 초기 질문 목록을 반환한다 — 선택 지시와 발화 조립이 공유하는 계약."""
    label = position_label(position)
    if label:
        return tuple(question.format(position=label) for question in _INITIAL_QUESTION_POOL)
    return tuple(question for question in _INITIAL_QUESTION_POOL if "{position}" not in question)


def selection_instructions(
    *, position: str | None = None, resume_context: str | None = None
) -> str:
    """초기 질문 선택 지시를 조립한다. LLM 출력은 질문 번호 하나뿐이다(발화는 코드가 조립)."""
    questions = "\n".join(
        f"{number}. {question}"
        for number, question in enumerate(question_pool(position), start=1)
    )
    parts = [
        "면접 초기 질문을 고릅니다. 아래 목록에서 지원자에게 가장 적절한 질문 하나를 골라 "
        f"그 번호만 출력하세요. 다른 텍스트 없이 숫자 하나만 답하세요.\n{questions}",
    ]
    if resume_context:
        parts.append(
            "다음은 지원자의 이력서 요약입니다. 어떤 질문이 적절할지 판단하는 데만 참고하세요.\n"
            f"{resume_context}"
        )
    return "\n\n".join(parts)
