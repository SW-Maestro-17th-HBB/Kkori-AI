"""평가기 테스트 — 가짜 결정성·출력 모델 검증·후처리(sanitize)·어휘집-문서 동기.

Bedrock 실호출은 테스트하지 않는다(이력서와 같은 방침) — 형태는 tool 강제가 보장하고,
품질은 수동 확인. 여기서는 LLM 없이 검증 가능한 규칙만 고정한다.
"""

from pathlib import Path

import pytest
from pydantic import ValidationError

from src.contract import ImprovementTask, QuestionAnswer, QuestionType
from src.report.evaluator import (
    MAX_TAGS_PER_ANSWER,
    TEXT_WEAKNESS_TAGS,
    AnswerEvaluation,
    EvaluatedAnswer,
    FakeEvaluator,
    TopicEvaluations,
    group_topics,
    sanitize_evaluation,
    validated_topic_result,
)

DOC = Path(__file__).parent.parent / "docs" / "requirements" / "report-evaluation" / "evaluation-criteria.md"


def qa(n: int = 1, parent: int | None = None,
       answer: str = "1차 캐시로 엔티티를 관리합니다.") -> QuestionAnswer:
    is_main = parent is None or parent == n
    return QuestionAnswer(
        questionNumber=n,
        parentQuestionNumber=parent if parent is not None else n,
        questionType=QuestionType.MAIN if is_main else QuestionType.TAIL,
        question="JPA 영속성 컨텍스트를 설명해주세요.",
        answer=answer,
    )


def evaluation(**overrides) -> AnswerEvaluation:
    base = dict(
        questionNumber=1,
        logicScore=70, specificityScore=70, technicalAccuracyScore=70,
        feedback="결론을 먼저 말하면 더 좋아요.",
        weaknessTags=["두괄식 부족"],
        improvementTasks=[ImprovementTask(title="결론부터 말하기", description="첫 문장에 핵심 결론 배치")],
    )
    return AnswerEvaluation(**{**base, **overrides})


# ---------------------------------------------------------------- 주제 묶기

def test_주제_묶기는_본질문과_꼬리를_묶고_번호순으로_정렬한다():
    pairs = [qa(1), qa(2), qa(3, parent=2), qa(4, parent=2), qa(5)]
    topics = group_topics(pairs)
    assert [[p.questionNumber for p in topic] for topic in topics] == [[1], [2, 3, 4], [5]]


# ---------------------------------------------------------------- FakeEvaluator

def test_가짜_기본값은_결정적이고_문답_번호를_반향한다():
    fake = FakeEvaluator()
    topic = [qa(1), qa(2, parent=1)]
    first, second = fake.evaluate_topic(topic), fake.evaluate_topic(topic)
    assert first == second
    assert [e.questionNumber for e in first] == [1, 2]  # 번호 반향 계약
    assert first[0].logicScore == 80
    assert fake.summarize([]) == fake.summarize([])


def test_가짜_질문번호별_결과와_예외_주입():
    injected = evaluation(logicScore=30)
    fake = FakeEvaluator(results={2: injected}, fail_on=frozenset({3}))

    results = fake.evaluate_topic([qa(1), qa(2, parent=1)])
    assert results[0].logicScore == 80  # 미주입 → 기본값
    assert results[1].logicScore == 30
    with pytest.raises(RuntimeError):
        fake.evaluate_topic([qa(3)])


# ---------------------------------------------------------------- 출력 모델 검증

@pytest.mark.parametrize("field", ["logicScore", "specificityScore", "technicalAccuracyScore"])
@pytest.mark.parametrize("invalid_score", [-1, 101])
def test_점수_범위_위반은_검증오류다(field, invalid_score):
    with pytest.raises(ValidationError):
        evaluation(**{field: invalid_score})


@pytest.mark.parametrize("boundary", [0, 100])
def test_점수_경계값은_허용된다(boundary):
    assert evaluation(logicScore=boundary).logicScore == boundary


# ---------------------------------------------------------------- sanitize (규칙의 결정적 집행)

def test_어휘집_밖_태그는_제거된다():
    result = sanitize_evaluation(evaluation(weaknessTags=["두괄식 부족", "발음 부정확"]))
    assert result.weaknessTags == ["두괄식 부족"]


def test_태그_3개_초과는_잘라낸다():
    result = sanitize_evaluation(evaluation(weaknessTags=list(TEXT_WEAKNESS_TAGS[:5])))
    assert len(result.weaknessTags) == MAX_TAGS_PER_ANSWER


def test_태그가_없으면_개선과제도_없다():
    result = sanitize_evaluation(evaluation(weaknessTags=["없는 태그"]))
    assert result.weaknessTags == []
    assert result.improvementTasks == []


def test_개선과제_2개_초과는_잘라낸다():
    tasks = [ImprovementTask(title=f"과제{i}", description="설명") for i in range(4)]
    result = sanitize_evaluation(evaluation(improvementTasks=tasks))
    assert len(result.improvementTasks) == 2


# ---------------------------------------------------------------- 주제 호출 결과 검증 (번호 대조)

def test_평가가_누락되면_오류다():
    raw = TopicEvaluations(evaluations=[evaluation(questionNumber=1)])
    with pytest.raises(ValueError):
        validated_topic_result(raw, [qa(1), qa(2, parent=1)])  # 문답 2개 ≠ 평가 1개


def test_번호_순서가_뒤바뀌면_오류다():
    raw = TopicEvaluations(
        evaluations=[evaluation(questionNumber=2), evaluation(questionNumber=1)]
    )
    with pytest.raises(ValueError):
        validated_topic_result(raw, [qa(1), qa(2, parent=1)])


def test_없는_번호를_창작하면_오류다():
    raw = TopicEvaluations(
        evaluations=[evaluation(questionNumber=1), evaluation(questionNumber=9)]
    )
    with pytest.raises(ValueError):
        validated_topic_result(raw, [qa(1), qa(2, parent=1)])


def test_번호가_일치하면_원소별로_정리되어_반환된다():
    raw = TopicEvaluations(
        evaluations=[
            evaluation(questionNumber=1, weaknessTags=["두괄식 부족", "없는 태그"]),
            evaluation(questionNumber=2),
        ]
    )
    results = validated_topic_result(raw, [qa(1), qa(2, parent=1)])
    assert [e.questionNumber for e in results] == [1, 2]
    assert results[0].weaknessTags == ["두괄식 부족"]  # sanitize 적용됨


# ---------------------------------------------------------------- 어휘집-문서 동기

def test_태그_어휘집은_설계_문서와_일치한다():
    """코드 상수(프롬프트 원천)와 evaluation-criteria.md §2.1 표가 어긋나면 실패한다."""
    section = DOC.read_text(encoding="utf-8").split("### 2.1")[1].split("### 2.2")[0]
    doc_tags = []
    for line in section.splitlines():
        if not line.startswith("|"):
            continue
        cell = line.split("|")[1].strip()
        if cell and cell != "태그" and set(cell) != {"-"}:
            doc_tags.append(cell)
    assert doc_tags == list(TEXT_WEAKNESS_TAGS)


# ---------------------------------------------------------------- EvaluatedAnswer

def test_총평_입력_단위는_문답과_평가를_묶는다():
    answer = EvaluatedAnswer(qa=qa(1), evaluation=evaluation())
    assert answer.qa.questionNumber == 1
    assert answer.evaluation.weaknessTags == ["두괄식 부족"]
