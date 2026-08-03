"""면접 대본(INTERVIEW_TRANSCRIPTS) jsonb 계약.

대본은 세션당 1행이며, 내용은 발화(utterance) 배열 JSON 이다. 스키마의 소유는
면접 도메인·에이전트다(기록 주체) — 워커는 읽기 전용 소비자로서 아래 형태를 전제한다.
값 집합(speaker/questionType)의 직렬화 형식은 면접 도메인과 합의 전 잠정.

핵심 규칙 (백엔드 PRD §1 기타 — 2026-07 확정):
- questionNumber 는 질문-답변 쌍의 전체 순번 (꼬리 질문 포함 연속 증가, 항상 유일)
  — 질문-답변 매칭(같은 번호 + speaker)과 답변별 평가 조인의 키.
- parentQuestionNumber 는 소속 본질문의 번호 (본질문은 자기 번호와 동일)
  — 주제 맥락 묶음용. 평가 시 같은 부모의 선행 문답을 맥락으로 쓴다.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class Speaker(str, Enum):
    INTERVIEWER = "INTERVIEWER"
    USER = "USER"


class QuestionType(str, Enum):
    MAIN = "MAIN"
    TAIL = "TAIL"


class Utterance(BaseModel):
    """대본의 발화 1개. spokenAt 은 ISO-8601 문자열 — 정렬 시 실제 시각으로 파싱한다
    (문자열 비교는 오프셋·소수점 자릿수가 다르면 시간순을 보장하지 못한다)."""

    questionNumber: int
    parentQuestionNumber: int
    speaker: Speaker
    questionType: QuestionType
    content: str
    spokenAt: str


class QuestionAnswer(BaseModel):
    """평가 단위 — 같은 questionNumber 의 발화를 묶은 질문-답변 쌍."""

    questionNumber: int
    parentQuestionNumber: int
    questionType: QuestionType
    question: str
    answer: str


def group_utterances(utterances: list[Utterance]) -> list[QuestionAnswer]:
    """발화 배열 → 질문-답변 쌍 목록 (questionNumber 오름차순).

    같은 번호에서 INTERVIEWER 발화는 질문으로, USER 발화는 시간순으로 이어붙여
    답변으로 만든다. USER 발화가 없는 번호(질문만 하고 종료 등)는 answer 가 빈 문자열.
    """
    numbers: dict[int, list[Utterance]] = {}
    for utterance in utterances:
        numbers.setdefault(utterance.questionNumber, []).append(utterance)

    pairs: list[QuestionAnswer] = []
    for number in sorted(numbers):
        group = sorted(numbers[number], key=lambda u: datetime.fromisoformat(u.spokenAt))
        question = " ".join(u.content for u in group if u.speaker == Speaker.INTERVIEWER)
        answer = " ".join(u.content for u in group if u.speaker == Speaker.USER)
        first = group[0]
        pairs.append(QuestionAnswer(
            questionNumber=number,
            parentQuestionNumber=first.parentQuestionNumber,
            questionType=first.questionType,
            question=question,
            answer=answer,
        ))
    return pairs
