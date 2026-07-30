"""면접 답변 평가기 — 인터페이스 + 가짜(fake)/Bedrock 구현.

평가 기준의 정의 원천은 `docs/requirements/report-evaluation/evaluation-criteria.md` 다 —
프롬프트의 루브릭 앵커·태그 어휘집·모름-답변 서열은 그 문서를 그대로 옮긴 것이며,
기준 변경은 문서를 먼저 고치고 프롬프트·어휘 상수와 한 커밋으로 반영한다 (문서 §2.3).

- 가짜: LLM 없이 파이프라인 로직을 결정적으로 테스트하기 위한 것 (providers.py 선례).
- Bedrock: 실제 호출 — tool 강제로 형태를 보장하고, 후처리(sanitize)가 어휘·개수 규칙을
  결정적으로 집행한다. 점수 범위(0~100)·배열 길이 위반만 오류(재시도 대상)로 취급한다.
- 메서드는 동기다 — 파이프라인이 `asyncio.to_thread` 로 감싼다 (이력서 파이프라인 선례).
- 호출 단위는 **주제(본질문+꼬리 질문들) 1회** + 세션 총평 1회. 답변별 호출 대비 채점
  기준표의 반복 전송이 주제 수만큼으로 줄고, 꼬리 질문 평가에 필요한 선행 문답이 같은
  호출에 자연히 포함된다. 평가 단위는 여전히 답변별(출력 배열의 원소)이며, 배열
  누락·초과는 길이 검증으로 거른다(이력서 Enricher 선례). 순차 실행 — 동시화는 실측 후.
"""

from __future__ import annotations

import json
import logging
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, Field

from src.config import Settings
from src.contract import ImprovementTask, QuestionAnswer, QuestionType

logger = logging.getLogger(__name__)

# 약점 태그 어휘집 — 텍스트 기반 12종 (정의 원천: evaluation-criteria.md §2.1, 순서 동일)
TEXT_WEAKNESS_TAGS: tuple[str, ...] = (
    "두괄식 부족",
    "근거 부족",
    "구체성 부족",
    "장황함",
    "질문 이탈",
    "기술 개념 오류",
    "깊이 부족",
    "용어 남용",
    "경험 연결 부족",
    "답변 미완성",
    "앞뒤 불일치",
    "대안 검토 부족",
)
MAX_TAGS_PER_ANSWER = 3  # 문서 §2 — 답변당 0~3개
MAX_TASKS_PER_ANSWER = 2  # 문서 §4 — 답변당 0~2개
# 과제 제목 길이: 문서 §4 의 25자는 프롬프트로 지시하는 권고고, 여기 상한은 UI 를 깨는
# 극단값만 거르는 안전선이다 — 잘라내면 문장이 깨지므로 초과 과제는 통째로 버린다(관대 처리).
MAX_TASK_TITLE_LEN = 50


class AnswerEvaluation(BaseModel):
    """답변 1개의 평가 결과 — tool-use 스키마이자 검증기 (0~100 위반은 오류).

    questionNumber 는 위치 신뢰 대신 번호 대조로 문답-평가 대응을 검증하기 위한 반향(echo).
    """

    questionNumber: int = Field(description="평가한 문답의 번호 — 입력의 [문답 N] 표기를 그대로")
    logicScore: int = Field(ge=0, le=100, description="논리성 0~100 — 앵커 선택 후 ±10 조정")
    specificityScore: int = Field(ge=0, le=100, description="구체성 0~100 — 앵커 선택 후 ±10 조정")
    technicalAccuracyScore: int = Field(
        ge=0, le=100, description="기술 정확성 0~100 — 앵커 선택 후 ±10 조정"
    )
    feedback: str = Field(description="1~2문장 — 가장 점수를 깎은 지적 1개 (전 축 80 이상이면 칭찬)")
    weaknessTags: list[str] = Field(
        default_factory=list, description="약점 태그 0~3개 — 어휘집 목록에서만 선택"
    )
    improvementTasks: list[ImprovementTask] = Field(
        default_factory=list, description="개선 과제 0~2개 — 약점 태그가 있을 때만"
    )


class TopicEvaluations(BaseModel):
    """주제 1회 호출의 tool-use 스키마 — 문답 순서와 같은 순서·길이의 평가 배열."""

    evaluations: list[AnswerEvaluation]


class EvaluatedAnswer(BaseModel):
    """총평·집계의 입력 단위 — 질문-답변과 그 평가의 쌍."""

    qa: QuestionAnswer
    evaluation: AnswerEvaluation


def group_topics(pairs: list[QuestionAnswer]) -> list[list[QuestionAnswer]]:
    """질문-답변 목록 → 주제(같은 parentQuestionNumber) 묶음, 주제·문답 모두 번호 오름차순.

    입력은 group_utterances() 결과(questionNumber 오름차순)를 전제한다.
    """
    topics: dict[int, list[QuestionAnswer]] = {}
    for qa in pairs:
        topics.setdefault(qa.parentQuestionNumber, []).append(qa)
    return [topics[parent] for parent in sorted(topics)]


@runtime_checkable
class Evaluator(Protocol):
    """답변 평가 LLM — 주제(본질문+꼬리) 단위 호출, 문답별 평가 배열 반환."""

    def evaluate_topic(self, topic: list[QuestionAnswer]) -> list[AnswerEvaluation]: ...

    def summarize(self, answers: list[EvaluatedAnswer]) -> str: ...


def sanitize_evaluation(evaluation: AnswerEvaluation) -> AnswerEvaluation:
    """어휘집·개수 규칙의 결정적 집행 — LLM 일탈을 실패 대신 정리로 흡수한다.

    태그·과제는 부가 정보라 관대하게 처리한다(전체 재호출 비용보다 싸다):
    어휘 밖 태그 제거, 3개 초과 잘라냄, 태그가 없으면 과제도 없음(문서 §4).
    """
    tags = [t for t in evaluation.weaknessTags if t in TEXT_WEAKNESS_TAGS]
    dropped = [t for t in evaluation.weaknessTags if t not in TEXT_WEAKNESS_TAGS]
    if dropped:
        logger.warning("어휘집 밖 약점 태그 제거: %s", dropped)
    tags = tags[:MAX_TAGS_PER_ANSWER]
    tasks = evaluation.improvementTasks if tags else []
    long_titles = [t.title for t in tasks if len(t.title) > MAX_TASK_TITLE_LEN]
    if long_titles:
        # 제목 원문은 로그에 남기지 않는다 — 답변에서 파생된 자유 텍스트라 개인정보가
        # 섞일 수 있다(레포 로그 규칙). 원문 확인이 필요하면 저장된 평가로 추적한다.
        logger.warning("제목 길이 초과 개선 과제 %d개 제거", len(long_titles))
    tasks = [t for t in tasks if len(t.title) <= MAX_TASK_TITLE_LEN][:MAX_TASKS_PER_ANSWER]
    return evaluation.model_copy(update={"weaknessTags": tags, "improvementTasks": tasks})


def validated_topic_result(
    raw: TopicEvaluations, topic: list[QuestionAnswer]
) -> list[AnswerEvaluation]:
    """주제 호출 결과 검증 — 반향된 questionNumber 목록이 보낸 문답 번호 목록과 정확히
    일치해야 한다(순서 포함). 불일치는 오류(재시도 대상), 일치하면 원소별 sanitize.

    번호 대조는 개수 불일치(누락·초과)뿐 아니라 순서 뒤바뀜·번호 창작까지 잡는다 —
    어긋나면 어느 평가가 어느 답변 것인지 대응을 신뢰할 수 없으므로 관대하게 살리지
    않는다 (이력서 Enricher 의 배열 길이 검증 선례를 번호 대조로 강화한 것).
    """
    sent = [qa.questionNumber for qa in topic]
    received = [e.questionNumber for e in raw.evaluations]
    if received != sent:
        raise ValueError(f"평가-문답 대응 불일치: 보낸 번호 {sent} ≠ 받은 번호 {received}")
    return [sanitize_evaluation(e) for e in raw.evaluations]


class FakeEvaluator:
    """정해진 평가를 반환하는 가짜 — questionNumber 별 결과·예외 주입으로 케이스를 재현한다."""

    def __init__(
        self,
        results: dict[int, AnswerEvaluation] | None = None,
        default: AnswerEvaluation | None = None,
        summary: str = "전반적으로 핵심을 짚는 안정적인 답변이었습니다.",
        fail_on: frozenset[int] = frozenset(),
    ) -> None:
        self._results = results or {}
        self._default = default if default is not None else AnswerEvaluation(
            questionNumber=0,  # 자리표시 — 반환 시 문답 번호로 덮어씀
            logicScore=80,
            specificityScore=80,
            technicalAccuracyScore=80,
            feedback="핵심을 짚어 잘 설명했어요.",
        )
        self._summary = summary
        self._fail_on = fail_on

    def evaluate_topic(self, topic: list[QuestionAnswer]) -> list[AnswerEvaluation]:
        for qa in topic:
            if qa.questionNumber in self._fail_on:
                raise RuntimeError(f"주입된 평가 실패 (questionNumber={qa.questionNumber})")
        return [
            self._results.get(qa.questionNumber, self._default).model_copy(
                update={"questionNumber": qa.questionNumber}  # 번호 반향 계약을 가짜도 지킨다
            )
            for qa in topic
        ]

    def summarize(self, answers: list[EvaluatedAnswer]) -> str:
        return self._summary


# ---------------------------------------------------------------- Bedrock 구현

_RUBRIC_AND_RULES = """\
## 점수 산정 — 루브릭 앵커
각 축마다 답변에 가장 가까운 앵커를 고른 뒤, 그 앵커 값에서 ±10 이내로 조정한 정수를 매긴다.

### 논리성 (logicScore) — 주장과 근거의 구조
- 100: 결론을 먼저 말하고, 근거가 결론을 직접 뒷받침하며, 흐름에 비약이 없다
- 80: 구조(주장-근거)는 갖췄으나 일부 연결이 느슨하거나 결론이 뒤에 온다
- 60: 주장과 근거가 있지만 순서가 뒤섞여 흐름을 따라가기 어렵다
- 40: 사실 나열 위주로, 무엇을 주장하는지 흐릿하다
- 20: 앞뒤가 모순되거나 질문과 무관한 방향으로 전개된다

### 구체성 (specificityScore) — 실제 경험·세부의 밀도
- 100: 수치·구체적 기술명·상황이 담긴 실제 경험으로 답한다
- 80: 사례는 있으나 세부(수치·본인 역할·결과)가 부족하다
- 60: 일반론이 중심이고 사례는 짧게 스치듯 언급된다
- 40: 추상적 서술 위주로, 답변자가 겪은 일인지 판별이 어렵다
- 20: 원론·교과서 문장뿐, 경험이 없다

### 기술 정확성 (technicalAccuracyScore) — 개념의 옳음과 깊이
- 100: 개념이 정확하고, 전제·한계·트레이드오프까지 언급한다
- 80: 대체로 정확하나 사소한 부정확이나 과한 단순화가 있다
- 60: 핵심은 맞지만 중요한 부분을 얼버무리거나 깊이가 없다
- 40: 개념 혼동이 뚜렷하다 (용어와 실체가 어긋남)
- 20: 설명이 틀렸다

### 모르는 질문에 대한 답변 — 정직이 손해 보지 않는 서열
아는 척하다 틀리는 것이 최악이고, 정직한 인정은 그보다 위다.
- 모름 인정 + 아는 범위에서 접근 시도: 시도 내용을 정상 루브릭으로 평가 (60대까지 가능)
- 순수한 모름 인정("모르겠습니다"만): 세 축 모두 30~40. "기술 개념 오류" 태그를 달지
  않는다(오류가 아니라 공백 — "깊이 부족"이 맞다). 피드백은 인정 자체를 긍정하고,
  아는 개념과 연결해 접근하는 시도를 권하는 내용으로 쓴다.
- 아는 척 틀린 설명: 기술 정확성 20 (최하)
- 무응답·중도 포기: 세 축 모두 20 이하

## 약점 태그 (weaknessTags) — 아래 12개에서만 선택한다 (자유 생성 금지), 답변당 0~3개
- 두괄식 부족: 결론·핵심이 답변 끝에 나오거나 나오지 않음
- 근거 부족: 주장은 있으나 수치·사례 등 뒷받침이 없음
- 구체성 부족: 서술이 추상적이고 실제 경험의 세부가 없음
- 장황함: 핵심 대비 분량이 과함, 같은 말 반복
- 질문 이탈: 질문의 요지와 다른 내용으로 답함
- 기술 개념 오류: 기술 설명이 부정확하거나 틀림
- 깊이 부족: 표면적 이해에 머무름 (왜/어떻게 설명 불가)
- 용어 남용: 전문 용어를 설명 없이 나열
- 경험 연결 부족: 이력서의 관련 경험이 있는데 활용하지 못함
- 답변 미완성: 말을 끝맺지 못하거나 중간에 포기
- 앞뒤 불일치: 같은 세션 내 다른 답변과 모순
- 대안 검토 부족: 선택·결정을 말하며 대안이나 트레이드오프를 언급하지 않음

## 피드백 (feedback)
- 1~2문장. 행동 가능한 지적 1개에 집중한다 — 여러 문제가 보여도 가장 점수를 깎은 것 하나.
- 문체는 존댓말 제안형("~하면 더 좋아요"), 비난·단정 금지.
- 잘한 답변(세 축 모두 80 이상)이면 강점을 짚는 칭찬 1문장으로 갈음한다.

## 개선 과제 (improvementTasks)
- 답변당 0~2개. 약점 태그와 연결된 것만 만든다 (태그 없는 답변엔 과제도 없음).
- title 은 행동 지시형 짧은 제목(25자 이내), description 은 구체적 실행 방법 1문장.
- 예: {"title": "결론부터 말하기 (PREP)", "description": "답변 첫 문장에 핵심 결론 배치"}
"""

_EVALUATE_PROMPT = """\
너는 개발자 채용 면접의 답변 평가자다. 아래 기준에 따라 한 주제의 문답 {count}개를
각각 평가해 save_evaluations 도구로 저장하라.

{rubric}

## 평가 대상 — 같은 주제의 문답 {count}개 (순서대로)
{topic_block}

## 출력 규칙
- evaluations 배열은 위 문답과 **같은 순서, 정확히 {count}개**여야 한다.
- 각 평가의 questionNumber 에는 해당 문답의 번호([문답 N] 의 N)를 그대로 적는다.
- 각 답변을 독립적으로 평가하되, 같은 주제의 앞 문답을 맥락으로 고려한다
  (특히 "질문 이탈"·"앞뒤 불일치" 판단, 꼬리 질문의 의도 파악).
"""

_SUMMARIZE_PROMPT = """\
너는 면접 리포트의 총평 작성자다. 한 세션의 답변별 평가 결과를 보고 3~5문장의
총평을 save_summary 도구로 저장하라.

구성 (이 순서를 지킨다):
1. 강점 1개 — 세션 전체에서 가장 잘한 지점 (없으면 시도 자체의 인정)
2. 약점 패턴 — 반복 등장한 약점 태그 기준 (단발성 실수는 언급하지 않음)
3. 다음 면접을 위한 조언 — 약점 패턴을 고치는 우선순위 1개

문체는 존댓말 제안형, 비난·단정 금지.

## 답변별 평가 결과
{payload}
"""


class _SummaryOutput(BaseModel):
    """총평 tool-use 스키마 — 문자열 하나를 확실히 추출하기 위한 껍데기."""

    summary: str = Field(description="세션 총평 3~5문장")


def _render_topic(topic: list[QuestionAnswer]) -> str:
    lines = []
    for qa in topic:
        kind = "꼬리 질문" if qa.questionType is QuestionType.TAIL else "본질문"
        lines.append(
            f"[문답 {qa.questionNumber}] ({kind})\n"
            f"질문: {qa.question}\n답변: {qa.answer or '(답변 없음)'}"
        )
    return "\n\n".join(lines)


class BedrockEvaluator:
    """Claude(Bedrock) 평가 — tool 강제로 형태를, 길이 검증으로 문답-평가 대응을 보장한다."""

    def __init__(self, settings: Settings) -> None:
        from anthropic import AnthropicBedrock

        self._client = AnthropicBedrock(aws_region=settings.bedrock_region)
        self._model_id = settings.evaluation_model_id

    def evaluate_topic(self, topic: list[QuestionAnswer]) -> list[AnswerEvaluation]:
        tool = {
            "name": "save_evaluations",
            "description": "한 주제의 문답별 평가 결과 배열을 저장한다.",
            "input_schema": TopicEvaluations.model_json_schema(),
        }
        prompt = _EVALUATE_PROMPT.format(
            rubric=_RUBRIC_AND_RULES,
            count=len(topic),
            topic_block=_render_topic(topic),
        )
        message = self._client.messages.create(
            model=self._model_id,
            max_tokens=2048,
            tools=[tool],
            tool_choice={"type": "tool", "name": "save_evaluations"},  # 형태 강제
            messages=[{"role": "user", "content": prompt}],
        )
        for block in message.content:
            if block.type == "tool_use":
                return validated_topic_result(
                    TopicEvaluations.model_validate(block.input), topic
                )
        raise ValueError("평가 응답에 tool_use 블록이 없음")

    def summarize(self, answers: list[EvaluatedAnswer]) -> str:
        payload = json.dumps(
            [
                {
                    "questionNumber": a.qa.questionNumber,
                    "question": a.qa.question,
                    "logicScore": a.evaluation.logicScore,
                    "specificityScore": a.evaluation.specificityScore,
                    "technicalAccuracyScore": a.evaluation.technicalAccuracyScore,
                    "weaknessTags": a.evaluation.weaknessTags,
                    "feedback": a.evaluation.feedback,
                }
                for a in answers
            ],
            ensure_ascii=False,
        )
        tool = {
            "name": "save_summary",
            "description": "면접 세션의 총평을 저장한다.",
            "input_schema": _SummaryOutput.model_json_schema(),
        }
        message = self._client.messages.create(
            model=self._model_id,
            max_tokens=1024,
            tools=[tool],
            tool_choice={"type": "tool", "name": "save_summary"},
            messages=[{"role": "user", "content": _SUMMARIZE_PROMPT.format(payload=payload)}],
        )
        for block in message.content:
            if block.type == "tool_use":
                return _SummaryOutput.model_validate(block.input).summary
        raise ValueError("총평 응답에 tool_use 블록이 없음")


# ---------------------------------------------------------------- 팩토리

def build_evaluator(settings: Settings) -> Evaluator:
    if settings.ai_provider == "fake":
        return FakeEvaluator()
    if settings.ai_provider == "bedrock":
        return BedrockEvaluator(settings)
    raise ValueError(f"알 수 없는 ai_provider: {settings.ai_provider}")
