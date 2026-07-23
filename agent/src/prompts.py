"""면접관 프롬프트 — 페르소나·음성 제약·초기 질문 지시. docs/prd/interview.md §2."""

from __future__ import annotations

# 시스템 프롬프트는 역할·지시만 담는다. 지원 직무·이력서 요약은 초기 질문 지시에서 별도 주입.
INTERVIEWER_INSTRUCTIONS = (
    "당신은 AI 모의 면접관 '꼬리'입니다. 진지하되 적대적이지 않은 기술 면접관으로서 "
    "항상 한국어로 대화합니다. 모든 발화는 음성으로 전달됩니다. "
    "한두 문장으로 간결하게 말하고, 마크다운이나 리스트나 이모지나 특수문자 없이 "
    "말하듯 자연스러운 구어체를 사용하세요. "
    "지원자는 대부분 신입 또는 주니어입니다. 재직 경력을 전제한 표현을 쓰지 마세요."
)

# 초기 질문 목록 — 유형별 2개(자기소개/지원동기/경험개괄/강점). LLM이 질문을 생성하지 않고
# 이 검수된 목록에서 고르기만 한다(few-shot 생성은 예시를 어중간하게 섞은 질문이 나와 폐기).
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


def _question_pool(position: str | None) -> tuple[str, ...]:
    if position:
        return tuple(question.format(position=position) for question in _INITIAL_QUESTION_POOL)
    return tuple(question for question in _INITIAL_QUESTION_POOL if "{position}" not in question)


def initial_question_instructions(
    *, position: str | None = None, resume_context: str | None = None
) -> str:
    """초기 질문 지시를 조립한다. 직무·이력서 요약이 없어도 성립한다(폴백)."""
    questions = "\n".join(f"- {question}" for question in _question_pool(position))
    parts = [
        "면접을 시작합니다. 아래 질문 목록 중에서 지원자에게 가장 적절한 것을 하나 골라, "
        "가볍게 인사한 뒤 물어보세요. 목록에 없는 새로운 질문을 만들지 마세요. "
        f"고른 질문은 어투를 자연스럽게 다듬어도 되지만 내용은 유지하세요.\n{questions}",
    ]
    if resume_context:
        parts.append(
            "다음은 지원자의 이력서 요약입니다. 어떤 질문이 적절할지 판단하는 데만 참고하고, "
            f"질문에 이력서의 세부 경험을 언급하지는 마세요.\n{resume_context}"
        )
    return "\n\n".join(parts)
