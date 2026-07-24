"""턴별 LLM 입력 컨텍스트 구성 — 선택 주입·직렬화·절단. docs/prd/follow-up-question.md §4.

- Orchestrator: 번호 태그 텍스트 직렬화(`[Q{n}|root{p}]`·`[A{n}]`) — role 배열에는 질문
  번호·줄기 경계가 없어 refQuestionNumber를 반환할 근거가 없다.
- Interview: role 메시지(질문=assistant, 답변=user) — 자연스러운 대화 이어가기.
- 절단은 프롬프트 주입 시에만 적용한다. 대화 로그 원문은 보존된다.
"""

from __future__ import annotations

import math

from src.interview.conversation_log import ConversationLog, Speaker, Utterance

# 한국어 보수 토큰 추정 계수 — 영문식 chars/4는 한국어를 크게 과소 추정하므로
# 음절당 1토큰에 가까운 안전 마진을 둔다 [미확정 — 실측 조정]
_CHARS_PER_TOKEN = 1.5

# 절단 표식 — 주입 시 개별 발화가 상한을 넘으면 앞부분을 유지하고 뒤를 자른다
# (절단 방향 [미확정 — 실측 조정])
_CLIP_MARKER = " …(중략)"


def estimate_tokens(text: str) -> int:
    """문자 기반 보수 추정 — 실제보다 크게 잡아 예산 초과를 방지한다."""
    return math.ceil(len(text) / _CHARS_PER_TOKEN)


def _clip(content: str, cap_tokens: int) -> str:
    max_chars = int(cap_tokens * _CHARS_PER_TOKEN)
    if len(content) <= max_chars:
        return content
    return content[:max_chars] + _CLIP_MARKER


def _tagged_line(utterance: Utterance, cap_tokens: int) -> str:
    content = _clip(utterance.content, cap_tokens)
    if utterance.speaker is Speaker.INTERVIEWER:
        return (
            f"[Q{utterance.question_number}"
            f"|root{utterance.parent_question_number}] {content}"
        )
    return f"[A{utterance.question_number}] {content}"


def _role_message(utterance: Utterance, cap_tokens: int) -> tuple[str, str]:
    role = "assistant" if utterance.speaker is Speaker.INTERVIEWER else "user"
    return role, _clip(utterance.content, cap_tokens)


def orchestrator_context(
    log: ConversationLog, *, token_budget: int, utterance_token_cap: int
) -> str:
    """현재 줄기 + 이전 줄기 전체의 번호 태그 직렬화 (예산 초과 시 절단).

    절단 우선순위 (PRD §4): (1) 오래된 줄기부터 통째로 제외 → (2) 현재 줄기 안에서
    오래된 발화부터 제외하되 직전 질문+답변은 항상 포함 → 개별 발화 절단은 _clip이
    직렬화 시점에 상시 적용. 직전 질문+답변은 예산을 넘더라도 반환한다(최소 보장).
    """
    roots = log.branch_roots()
    if not roots:
        return ""

    branch_lines = [
        [_tagged_line(u, utterance_token_cap) for u in log.branch(root)]
        for root in roots
    ]

    def _joined(lines_2d: list[list[str]]) -> str:
        return "\n".join(line for lines in lines_2d for line in lines)

    # (1) 오래된 줄기 제외 — 현재(마지막) 줄기는 항상 유지
    start = 0
    while (
        start < len(branch_lines) - 1
        and estimate_tokens(_joined(branch_lines[start:])) > token_budget
    ):
        start += 1
    kept = branch_lines[start:]
    if estimate_tokens(_joined(kept)) <= token_budget:
        return _joined(kept)

    # (2) 현재 줄기 내부 절단 — 직전 질문부터의 꼬리(질문+답변들)는 항상 유지
    current_branch = log.branch(roots[-1])
    tail_start = max(
        i for i, u in enumerate(current_branch) if u.speaker is Speaker.INTERVIEWER
    )
    current_lines = kept[-1]
    drop = 0
    while (
        drop < tail_start
        and estimate_tokens(_joined([current_lines[drop:]])) > token_budget
    ):
        drop += 1
    return _joined([current_lines[drop:]])


def follow_up_messages(
    log: ConversationLog, *, utterance_token_cap: int
) -> list[tuple[str, str]]:
    """FOLLOW_UP 생성용 — 현재 줄기 전체를 role 메시지로."""
    return [_role_message(u, utterance_token_cap) for u in log.current_branch()]


def recent_branch_messages(
    log: ConversationLog, *, n: int, utterance_token_cap: int
) -> list[tuple[str, str]]:
    """NEXT_TOPIC 생성용 — 현재 줄기를 포함한 최근 n개 줄기를 role 메시지로."""
    return [
        _role_message(u, utterance_token_cap)
        for branch in log.recent_branches(n)
        for u in branch
    ]


def branch_text(
    log: ConversationLog, root_number: int, *, utterance_token_cap: int
) -> str:
    """CONSISTENCY 참조 줄기 주입용 — 지정 줄기의 번호 태그 직렬화."""
    return "\n".join(
        _tagged_line(u, utterance_token_cap) for u in log.branch(root_number)
    )
