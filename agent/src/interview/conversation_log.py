"""세션 대화 로그 — 발화 객체 스키마와 메모리 주 사본. docs/prd/follow-up-question.md §4.

발화 객체의 불변식은 이 모듈이 강제한다. 메모리 로그가 모든 읽기(컨텍스트 구성)의
단일 원천이고, Redis write-through는 조립 코드가 append 반환값을 별도 enqueue한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum


class Speaker(StrEnum):
    INTERVIEWER = "INTERVIEWER"
    CANDIDATE = "CANDIDATE"


class QuestionType(StrEnum):
    INITIAL = "initial"
    TOPIC = "topic"
    FOLLOW_UP = "followup"
    FINAL = "final"  # 마무리 국면의 마지막 질문 — docs/prd/interview-end.md §2
    CLOSING = "closing"  # 클로징 인사 — 질문이 아니므로 번호 없음


class Action(StrEnum):
    """Orchestrator 액션 — docs/prd/follow-up-question.md §2.
    FINAL_QUESTION·END는 마무리 단계에만 허용된다 — docs/prd/interview-end.md §2."""

    FOLLOW_UP = "FOLLOW_UP"
    NEXT_TOPIC = "NEXT_TOPIC"
    FINAL_QUESTION = "FINAL_QUESTION"
    END = "END"


class FollowUpType(StrEnum):
    """꼬리질문 분류 체계 — docs/prd/follow-up-question.md §2."""

    DEEPEN = "DEEPEN"
    CONCRETE = "CONCRETE"
    VERIFY = "VERIFY"
    BOUNDARY = "BOUNDARY"
    CONSISTENCY = "CONSISTENCY"


@dataclass(frozen=True)
class Utterance:
    """transcript 발화 객체. 해당 없는 필드는 None(직렬화 시 생략 — null 금지).

    질문 번호는 closing에만 없다 — "질문이면 번호 있음(final 포함), 인사면 없음"
    (docs/prd/interview-end.md §2).
    """

    question_number: int | None
    parent_question_number: int | None
    speaker: Speaker
    content: str
    spoken_at: datetime
    question_type: QuestionType | None = None
    follow_up_type: FollowUpType | None = None
    reason: str | None = None
    ref_question_number: int | None = None

    def __post_init__(self) -> None:
        if not self.content:
            raise ValueError("content는 비어 있을 수 없다")
        if self.spoken_at.utcoffset() != timedelta(0):
            # naive는 utcoffset()이 None이라 함께 거부된다 — spokenAt 직렬화("Z")의 전제
            raise ValueError("spoken_at은 UTC(offset 0)인 timezone-aware여야 한다")

        if self.question_type is QuestionType.CLOSING:
            if self.speaker is not Speaker.INTERVIEWER:
                raise ValueError("closing은 INTERVIEWER 발화여야 한다")
            if self.question_number is not None or self.parent_question_number is not None:
                raise ValueError("closing은 질문 번호를 갖지 않는다 (줄기 체계 밖)")
            if self.follow_up_type is not None or self.ref_question_number is not None:
                raise ValueError("closing은 질문 메타데이터를 가질 수 없다")
            return

        if self.question_number is None or self.parent_question_number is None:
            raise ValueError("closing 외 발화는 질문 번호가 필요하다")
        if self.question_number < 1:
            raise ValueError("question_number는 1 이상이어야 한다")

        if self.speaker is Speaker.CANDIDATE:
            if (
                self.question_type is not None
                or self.follow_up_type is not None
                or self.reason is not None
                or self.ref_question_number is not None
            ):
                raise ValueError("CANDIDATE 발화는 질문 메타데이터를 가질 수 없다")
            return

        if self.question_type is None:
            raise ValueError("INTERVIEWER 발화는 question_type이 필요하다")

        if self.question_type is QuestionType.FOLLOW_UP:
            if self.parent_question_number >= self.question_number:
                raise ValueError("꼬리질문의 parent는 먼저 커밋된 줄기 루트여야 한다")
            if self.follow_up_type is None:
                raise ValueError("followup은 follow_up_type이 필요하다")
        else:
            if self.parent_question_number != self.question_number:
                raise ValueError("루트 질문(initial·topic)은 parent=self여야 한다")
            if self.follow_up_type is not None:
                raise ValueError("initial·topic은 follow_up_type을 가질 수 없다")
            if self.question_type is QuestionType.INITIAL and self.reason is not None:
                raise ValueError("initial은 reason을 가질 수 없다")

        if self.follow_up_type is FollowUpType.CONSISTENCY:
            if self.ref_question_number is None:
                raise ValueError("CONSISTENCY는 ref_question_number가 필요하다")
        elif self.ref_question_number is not None:
            raise ValueError("ref_question_number는 CONSISTENCY에만 존재한다")

    def to_json_dict(self) -> dict:
        """camelCase 직렬화 — 해당 없는 필드는 키 자체를 생략한다."""
        data: dict = {}
        if self.question_number is not None:
            data["questionNumber"] = self.question_number
        if self.parent_question_number is not None:
            data["parentQuestionNumber"] = self.parent_question_number
        data["speaker"] = str(self.speaker)
        if self.question_type is not None:
            data["questionType"] = str(self.question_type)
        if self.follow_up_type is not None:
            data["followUpType"] = str(self.follow_up_type)
        if self.reason is not None:
            data["reason"] = self.reason
        if self.ref_question_number is not None:
            data["refQuestionNumber"] = self.ref_question_number
        data["content"] = self.content
        data["spokenAt"] = (
            self.spoken_at.isoformat(timespec="seconds").replace("+00:00", "Z")
        )
        return data


class ConversationLog:
    """메모리 주 사본 — append-only, 모든 조회의 단일 원천."""

    def __init__(self) -> None:
        self._utterances: list[Utterance] = []

    @property
    def utterances(self) -> tuple[Utterance, ...]:
        return tuple(self._utterances)

    def append_question(
        self,
        *,
        question_number: int,
        parent_question_number: int,
        question_type: QuestionType,
        content: str,
        spoken_at: datetime,
        follow_up_type: FollowUpType | None = None,
        reason: str | None = None,
        ref_question_number: int | None = None,
    ) -> Utterance:
        if question_number != self.last_question_number() + 1:
            raise ValueError("questionNumber는 공백·역전 없이 1씩 증가해야 한다")
        if question_type is QuestionType.FOLLOW_UP:
            # parent는 체인(직전 꼬리질문)이 아니라 현재 줄기의 루트여야 한다 (PRD §4)
            root = self.question_for(parent_question_number)
            if root is None or root.question_type not in (
                QuestionType.INITIAL,
                QuestionType.TOPIC,
            ):
                raise ValueError("꼬리질문의 parent는 존재하는 줄기 루트(initial·topic)여야 한다")
            if parent_question_number != self.current_root():
                raise ValueError("꼬리질문은 현재 줄기에만 붙는다 — 이전 줄기 루트 참조 불가")
        utterance = Utterance(
            question_number=question_number,
            parent_question_number=parent_question_number,
            speaker=Speaker.INTERVIEWER,
            content=content,
            spoken_at=spoken_at,
            question_type=question_type,
            follow_up_type=follow_up_type,
            reason=reason,
            ref_question_number=ref_question_number,
        )
        self._utterances.append(utterance)
        return utterance

    def append_closing(
        self, content: str, spoken_at: datetime, *, reason: str | None = None
    ) -> Utterance:
        """클로징 인사 적재 — 질문 번호 없는 INTERVIEWER 발화 (docs/prd/interview-end.md §2).

        reason은 END가 Orchestrator 판단일 때만 존재한다(기존 reason 규칙과 일관).
        """
        utterance = Utterance(
            question_number=None,
            parent_question_number=None,
            speaker=Speaker.INTERVIEWER,
            content=content,
            spoken_at=spoken_at,
            question_type=QuestionType.CLOSING,
            reason=reason,
        )
        self._utterances.append(utterance)
        return utterance

    def append_answer(self, content: str, spoken_at: datetime) -> Utterance:
        """현재 질문 번호·parent를 승계해 답변을 적재한다. 한 질문에 여러 답변 허용."""
        last_question = self._last_question()
        if last_question is None:
            raise ValueError("질문이 없는 상태에서는 답변을 적재할 수 없다")
        utterance = Utterance(
            question_number=last_question.question_number,
            parent_question_number=last_question.parent_question_number,
            speaker=Speaker.CANDIDATE,
            content=content,
            spoken_at=spoken_at,
        )
        self._utterances.append(utterance)
        return utterance

    # --- 조회 (전부 메모리 — 컨텍스트 구성·흐름 제어의 단일 원천) ---

    def last_question_number(self) -> int:
        last = self._last_question()
        return last.question_number if last else 0

    def current_root(self) -> int | None:
        last = self._last_question()
        return last.parent_question_number if last else None

    def branch(self, root_number: int) -> tuple[Utterance, ...]:
        """줄기 = 같은 parentQuestionNumber를 공유하는 발화 전체."""
        return tuple(
            u for u in self._utterances if u.parent_question_number == root_number
        )

    def current_branch(self) -> tuple[Utterance, ...]:
        root = self.current_root()
        return self.branch(root) if root is not None else ()

    def branch_roots(self) -> tuple[int, ...]:
        """등장 순서대로의 줄기 루트 번호 목록 (closing은 줄기 체계 밖 — 제외)."""
        roots: list[int] = []
        for u in self._utterances:
            root = u.parent_question_number
            if root is not None and root not in roots:
                roots.append(root)
        return tuple(roots)

    def previous_branches(self) -> tuple[tuple[Utterance, ...], ...]:
        """현재 줄기를 제외한 이전 줄기들(오래된 것부터)."""
        current = self.current_root()
        return tuple(
            self.branch(root) for root in self.branch_roots() if root != current
        )

    def recent_branches(self, n: int) -> tuple[tuple[Utterance, ...], ...]:
        """현재 줄기를 포함한 최근 n개 줄기(오래된 것부터)."""
        roots = self.branch_roots()[-n:] if n > 0 else ()
        return tuple(self.branch(root) for root in roots)

    def followup_count_in_current_branch(self) -> int:
        """현재 줄기의 꼬리질문 수 — M 상한 판정용(CONSISTENCY 포함)."""
        return sum(
            1
            for u in self.current_branch()
            if u.question_type is QuestionType.FOLLOW_UP
        )

    def question_for(self, question_number: int) -> Utterance | None:
        """번호로 질문 발화 조회 — CONSISTENCY ref 검증용."""
        for u in self._utterances:
            if (
                u.speaker is Speaker.INTERVIEWER
                and u.question_number == question_number
            ):
                return u
        return None

    def all_question_contents(self) -> tuple[str, ...]:
        """지금까지의 질문 원문 목록 — 주제 전환의 중복 방지 재료 (closing 제외)."""
        return tuple(
            u.content
            for u in self._utterances
            if u.speaker is Speaker.INTERVIEWER and u.question_number is not None
        )

    def has_topic_or_followup_question(self) -> bool:
        """본론 질문(topic·followup) 존재 여부 — 첫 답변 강제 전환 판정용."""
        return any(
            u.question_type in (QuestionType.TOPIC, QuestionType.FOLLOW_UP)
            for u in self._utterances
        )

    def _last_question(self) -> Utterance | None:
        for u in reversed(self._utterances):
            if u.speaker is Speaker.INTERVIEWER and u.question_number is not None:
                return u  # closing(번호 없음)은 질문이 아니다
        return None
