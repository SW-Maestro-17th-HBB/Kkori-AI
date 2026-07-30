"""대본 발화 → 질문-답변 쌍 묶기(group_utterances) 테스트.

규칙(백엔드 PRD §1 기타): questionNumber 는 쌍의 전체 순번(유일),
같은 번호의 INTERVIEWER 발화=질문, USER 발화=시간순 연결한 답변.
"""

from src.contract import QuestionType, Speaker, Utterance, group_utterances


def _u(number, parent, speaker, qtype, content, spoken_at):
    return Utterance(
        questionNumber=number,
        parentQuestionNumber=parent,
        speaker=speaker,
        questionType=qtype,
        content=content,
        spokenAt=spoken_at,
    )


def test_같은_번호의_발화가_질문과_답변으로_묶인다():
    pairs = group_utterances([
        _u(1, 1, Speaker.INTERVIEWER, QuestionType.MAIN, "자기소개를 부탁드립니다.", "2026-07-01T10:00:05Z"),
        _u(1, 1, Speaker.USER, QuestionType.MAIN, "3년차 백엔드 개발자입니다.", "2026-07-01T10:00:12Z"),
    ])
    assert len(pairs) == 1
    assert pairs[0].question == "자기소개를 부탁드립니다."
    assert pairs[0].answer == "3년차 백엔드 개발자입니다."


def test_쪼개진_사용자_발화는_시간순으로_이어붙인다():
    pairs = group_utterances([
        _u(2, 1, Speaker.INTERVIEWER, QuestionType.TAIL, "왜 그 기술을 썼나요?", "T1"),
        _u(2, 1, Speaker.USER, QuestionType.TAIL, "팀이 이미 익숙한 스택이었습니다.", "T3"),
        _u(2, 1, Speaker.USER, QuestionType.TAIL, "실시간 처리가 필요했고", "T2"),
    ])
    assert pairs[0].answer == "실시간 처리가 필요했고 팀이 이미 익숙한 스택이었습니다."


def test_질문번호_오름차순으로_정렬되고_꼬리는_부모번호를_유지한다():
    pairs = group_utterances([
        _u(3, 3, Speaker.INTERVIEWER, QuestionType.MAIN, "협업 경험은?", "T5"),
        _u(1, 1, Speaker.INTERVIEWER, QuestionType.MAIN, "자기소개?", "T1"),
        _u(2, 1, Speaker.INTERVIEWER, QuestionType.TAIL, "꼬리?", "T3"),
        _u(2, 1, Speaker.USER, QuestionType.TAIL, "답", "T4"),
    ])
    assert [p.questionNumber for p in pairs] == [1, 2, 3]
    assert pairs[1].parentQuestionNumber == 1
    assert pairs[1].questionType == QuestionType.TAIL


def test_답변_없는_질문은_answer가_빈문자열이다():
    pairs = group_utterances([
        _u(1, 1, Speaker.INTERVIEWER, QuestionType.MAIN, "마지막 질문입니다.", "T1"),
    ])
    assert pairs[0].answer == ""
