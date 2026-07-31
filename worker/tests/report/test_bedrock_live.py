"""리포트 평가 Bedrock 실호출 품질 확인 — 명시적으로 켰을 때만 실행한다 (과금 방지).

실행 방법 (worker/.env 에 AWS 자격이 있어야 함):
    KKORI_LIVE_BEDROCK=1 pytest worker/tests/test_report_bedrock_live.py -v -s

가짜 평가기로는 확인 불가한 것: 채점 기준표(evaluation-criteria.md)가 프롬프트로서
실제로 작동하는가. 골든 샘플 대본에 기준표가 구분해야 하는 케이스를 심어 두고,
실호출 결과가 기대 범위에 드는지 검증한다. 세부 점수는 실행마다 달라지므로
단정은 느슨한 범위·규칙 위반 여부만 하고, 전체 결과는 출력해 수동 확인한다.

케이스 설계 (§는 evaluation-criteria.md):
- 문답 1·2 (주제 1): 두괄식 + 수치·경험 답변 — 상위권 점수 기대
- 문답 3: 모름 인정 + 아는 개념으로 접근 시도 — §1.4 정상 채점(60대까지), "기술 개념 오류" 금지
- 문답 4: 아는 척 틀린 설명(GC 오개념) — §1.4 기술 정확성 최하
- 문답 5: 순수한 모름 인정 — §1.4 세 축 30~40대, "기술 개념 오류" 금지
- 문답 6: 질문 이탈 + 장황 — §2.1 해당 태그 기대
"""

import os
from pathlib import Path

import pytest

from src.config import Settings
from src.contract import Utterance, group_utterances
from src.report.evaluator import EvaluatedAnswer, group_topics

LIVE = os.environ.get("KKORI_LIVE_BEDROCK") == "1"
ENV_FILE = Path(__file__).parent.parent.parent / ".env"  # worker/.env

pytestmark = pytest.mark.skipif(
    not LIVE, reason="실 Bedrock 호출 테스트 — KKORI_LIVE_BEDROCK=1 로만 실행(과금)"
)


def _settings() -> Settings:
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        if key not in os.environ and ENV_FILE.exists():
            for line in ENV_FILE.read_text().splitlines():
                if line.startswith(f"{key}="):
                    os.environ[key] = line.split("=", 1)[1].strip()
    return Settings(_env_file=ENV_FILE, ai_provider="bedrock")


def _u(n, parent, speaker, qtype, content, t):
    return {"questionNumber": n, "parentQuestionNumber": parent, "speaker": speaker,
            "questionType": qtype, "content": content, "spokenAt": f"2026-07-30T10:{t}:00Z"}


GOLDEN_TRANSCRIPT = [
    _u(1, 1, "INTERVIEWER", "MAIN", "JPA 영속성 컨텍스트가 무엇인지 설명해주세요.", "00"),
    _u(1, 1, "USER", "MAIN",
       "영속성 컨텍스트는 엔티티를 관리하는 1차 캐시입니다. 같은 트랜잭션 안에서 같은 "
       "엔티티를 다시 조회하면 DB에 가지 않고 캐시에서 반환해 조회를 줄여줍니다. 실제로 "
       "주문 조회 API에서 반복 조회를 영속성 컨텍스트 재사용으로 바꿔 쿼리 수를 40% 줄인 "
       "경험이 있습니다. 또 변경 감지가 있어 커밋 시점에 바뀐 엔티티만 UPDATE가 나갑니다.", "01"),
    _u(2, 1, "INTERVIEWER", "TAIL", "그럼 flush와 commit의 차이는 무엇인가요?", "02"),
    _u(2, 1, "USER", "TAIL",
       "flush는 영속성 컨텍스트에 쌓인 변경 SQL을 DB로 보내는 것이고, commit은 그 변경을 "
       "확정하는 것입니다. flush가 됐어도 commit 전이면 다른 트랜잭션에서 보이지 않고 "
       "롤백도 가능합니다. JPQL을 실행하면 커밋 전에도 자동 flush가 일어나는 것으로 압니다.", "03"),
    _u(3, 3, "INTERVIEWER", "MAIN", "PostgreSQL의 MVCC가 어떻게 동작하는지 설명해주실 수 있나요?", "04"),
    _u(3, 3, "USER", "MAIN",
       "정확한 내부 구현은 잘 모르겠습니다. 다만 이름이 다중 버전 동시성 제어인 걸 보면, "
       "데이터를 덮어쓰는 대신 버전을 여러 개 두어 읽는 쪽이 잠금 없이 자기 시점의 버전을 "
       "읽게 하는 방식이 아닐까 추측합니다. 자바의 CopyOnWriteArrayList처럼 읽기와 쓰기가 "
       "서로를 막지 않게 하는 원리와 비슷할 것 같습니다.", "05"),
    _u(4, 4, "INTERVIEWER", "MAIN", "자바의 가비지 컬렉션 동작 방식을 설명해주세요.", "06"),
    _u(4, 4, "USER", "MAIN",
       "가비지 컬렉션은 개발자가 free()를 호출하면 그때 메모리를 회수하는 방식입니다. "
       "자바에서는 System.gc()를 호출해야만 메모리가 해제되고, 호출하지 않으면 프로그램이 "
       "끝날 때까지 메모리가 계속 쌓입니다. 그래서 저는 매 요청마다 System.gc()를 "
       "호출하도록 코드를 짭니다.", "07"),
    _u(5, 5, "INTERVIEWER", "MAIN", "Redis의 Sorted Set은 어떤 경우에 쓰나요?", "08"),
    _u(5, 5, "USER", "MAIN", "죄송합니다, 잘 모르겠습니다.", "09"),
    _u(6, 6, "INTERVIEWER", "MAIN", "프로젝트에서 트랜잭션 격리 수준을 조정한 경험이 있나요?", "10"),
    _u(6, 6, "USER", "MAIN",
       "제가 프로젝트를 할 때 팀워크가 정말 중요하다고 느꼈습니다. 저희 팀은 매일 아침 "
       "스크럼을 했는데요, 스크럼을 하면서 서로의 진행 상황을 공유했고 갈등이 있을 때도 "
       "대화로 풀었습니다. 협업 도구는 지라와 노션을 썼고 노션 정리는 제가 도맡아 했습니다. "
       "그리고 코드 리뷰 문화도 만들려고 노력했고 리뷰를 통해 많이 배웠다고 생각합니다. "
       "아무튼 팀워크가 제일 중요한 것 같습니다.", "11"),
]


def test_골든_샘플_평가_실호출():
    from src.report.evaluator import BedrockEvaluator

    evaluator = BedrockEvaluator(_settings())
    pairs = group_utterances([Utterance.model_validate(u) for u in GOLDEN_TRANSCRIPT])

    by_number = {}
    evaluated = []
    for topic in group_topics(pairs):
        results = evaluator.evaluate_topic(topic)  # 번호 반향·형태 검증은 내부에서 수행
        for qa, evaluation in zip(topic, results):
            by_number[qa.questionNumber] = evaluation
            evaluated.append(EvaluatedAnswer(qa=qa, evaluation=evaluation))

    print("\n===== 답변별 평가 (수동 확인용) =====")
    for n in sorted(by_number):
        e = by_number[n]
        print(f"[문답 {n}] 논리 {e.logicScore} / 구체 {e.specificityScore} / "
              f"기술 {e.technicalAccuracyScore} / 태그 {e.weaknessTags}")
        print(f"  피드백: {e.feedback}")
        for task in e.improvementTasks:
            print(f"  과제: {task.title} — {task.description}")

    # 기준표 위반 여부만 단정 (세부 점수는 실행마다 달라 수동 확인 영역)
    good = by_number[1]
    assert min(good.logicScore, good.specificityScore, good.technicalAccuracyScore) >= 60, \
        "두괄식+수치+경험 답변이 하위권이면 채점 기준표가 작동하지 않는 것"

    honest_try = by_number[3]
    assert "기술 개념 오류" not in honest_try.weaknessTags, "§1.4 — 모름 인정은 오류가 아니다"

    bluff = by_number[4]
    assert bluff.technicalAccuracyScore <= 40, "§1.4 — 아는 척 틀린 설명은 기술 정확성 최하"

    pure_admit = by_number[5]
    assert max(pure_admit.logicScore, pure_admit.specificityScore,
               pure_admit.technicalAccuracyScore) <= 50, "§1.4 — 순수 모름 인정은 30~40대"
    assert "기술 개념 오류" not in pure_admit.weaknessTags

    off_topic = by_number[6]
    assert {"질문 이탈", "장황함"} & set(off_topic.weaknessTags), \
        "격리 수준 질문에 팀워크로 답했는데 관련 태그가 없다"

    # 정직 서열: 아는 척 틀림 < 모름 인정 + 접근 시도 (기술 정확성 기준)
    assert bluff.technicalAccuracyScore < honest_try.technicalAccuracyScore

    summary = evaluator.summarize(evaluated)
    print(f"\n===== 세션 총평 =====\n{summary}")
    assert len(summary) >= 30
