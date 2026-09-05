"""전달력 분석 테스트 — DB 없이 픽스처 녹음과 합성 입력으로 규칙을 검증한다.

픽스처 `fixtures/candidate_voice.ogg` 는 macOS TTS(Yuna)로 만든 한국어 문장 4개를
다음 배치로 이은 44초 OGG/Opus 다: 무음 1s | A | 침묵 3s | B | 침묵 1s | C | 무음 15s | D | 무음 1s.
설계상 답변 중 침묵은 3s(긴 침묵)·1s 두 개이고, 15s 공백은 면접관 차례로 제외돼야 한다.
"""

import io
from pathlib import Path

import numpy as np
import pytest
import soundfile as sf

from src.report import audio as a

FIXTURE = Path(__file__).parent / "fixtures" / "candidate_voice.ogg"
FIXTURE_TEXT = (
    "영속성 컨텍스트는 엔티티를 관리하는 일차 캐시이고 트랜잭션 단위로 동작합니다. "
    "플러시는 변경 내용을 데이터베이스에 반영하는 것이고 커밋은 그것을 확정하는 것입니다. "
    "지연 로딩을 쓰면 연관된 엔티티를 실제로 접근할 때 조회하기 때문에 불필요한 쿼리를 줄일 수 있습니다. "
    "프로젝트에서는 배치 사이즈를 조정해서 엔 플러스 일 문제를 해결한 경험이 있습니다."
)


@pytest.fixture(scope="module")
def recording() -> bytes:
    return FIXTURE.read_bytes()


# --- 발화 구간 (Silero VAD 실행) ---------------------------------------------

def test_픽스처에서_문장_4개와_설계한_침묵이_잡힌다(recording):
    segments, duration = a.detect_speech(io.BytesIO(recording))

    assert 43 < duration < 45
    assert len(segments) == 4
    pauses = a.answer_pauses(segments)
    assert len(pauses) == 2  # 15s 공백은 면접관 차례로 제외
    assert pauses[0] == pytest.approx(3.0, abs=0.2)
    assert pauses[1] == pytest.approx(1.0, abs=0.2)
    gap_to_last = segments[3][0] - segments[2][1]
    assert gap_to_last > a.TURN_GAP_S


def test_분석은_결정적이다(recording):
    first = a.analyze(recording, FIXTURE_TEXT)
    second = a.analyze(recording, FIXTURE_TEXT)
    assert first == second
    assert first.score is not None


def test_픽스처_지표와_점수(recording):
    result = a.analyze(recording, FIXTURE_TEXT)
    m = result.metrics

    assert 20 < m.phonation_s < 26  # 문장 4개 발성 시간
    assert m.syllables == a.count_syllables(FIXTURE_TEXT)
    assert m.long_pause_count == 1
    assert m.pause_ratio < a.PAUSE_RATIO_FREE + 0.05
    assert a.RATE_SLOW <= m.articulation_rate <= a.RATE_FAST  # TTS 190wpm 은 적정 범위
    assert 80 <= result.score <= 100
    assert [t.tag for t in result.tags] == []


def test_대본이_짧으면_측정_불가(recording):
    result = a.analyze(recording, "짧은 답변")
    assert result.score is None
    assert result.tags == []
    assert result.metrics.measurable is False


def test_짧은_녹음은_측정_불가(recording):
    data, sr = sf.read(io.BytesIO(recording), dtype="float32")
    buf = io.BytesIO()
    sf.write(buf, data[: sr * 8], sr, format="OGG", subtype="OPUS")  # 앞 8초 — 문장 1개뿐

    result = a.analyze(buf.getvalue(), FIXTURE_TEXT)
    assert result.metrics.phonation_s < a.MIN_PHONATION_S
    assert result.score is None


def test_스테레오_파일도_모노처럼_처리한다(recording):
    data, sr = sf.read(io.BytesIO(recording), dtype="float32")
    buf = io.BytesIO()
    sf.write(buf, np.stack([data, data], axis=1), sr, format="OGG", subtype="OPUS")

    mono, _ = a.detect_speech(io.BytesIO(recording))
    stereo, _ = a.detect_speech(io.BytesIO(buf.getvalue()))
    assert len(stereo) == len(mono)
    for (s1, e1), (s2, e2) in zip(mono, stereo):
        assert s2 == pytest.approx(s1, abs=0.1) and e2 == pytest.approx(e1, abs=0.1)


# --- 리샘플 ------------------------------------------------------------------

def test_리샘플은_블록_크기와_무관하게_같다(recording):
    big = np.concatenate(list(a.resample_to_16k(io.BytesIO(recording), block_seconds=20)))
    small = np.concatenate(list(a.resample_to_16k(io.BytesIO(recording), block_seconds=1)))
    assert len(big) == len(small) == pytest.approx(44.05 * a.VAD_SAMPLE_RATE, abs=800)
    np.testing.assert_array_equal(big, small)


def test_16k_정수배가_아닌_샘플레이트는_거부():
    buf = io.BytesIO()
    sf.write(buf, np.zeros(44_100, dtype=np.float32), 44_100, format="WAV")
    with pytest.raises(ValueError, match="샘플레이트"):
        list(a.resample_to_16k(io.BytesIO(buf.getvalue())))


# --- 후처리 (합성 확률 — 모델 없이) -----------------------------------------

def _chunks(seconds: float) -> int:
    return int(seconds * a.VAD_SAMPLE_RATE / a.VAD_WINDOW)


def test_후처리_발화_시작_종료_임계와_패딩():
    probs = [0.1] * _chunks(1) + [0.9] * _chunks(2) + [0.1] * _chunks(1)
    total = len(probs) * a.VAD_WINDOW

    [(start, end)] = a.speech_segments(probs, total)
    pad = a.VAD_SPEECH_PAD_MS / 1000
    assert start == pytest.approx(1.0 - pad, abs=0.04)
    assert end == pytest.approx(3.0 + pad, abs=0.04)


def test_후처리_짧은_소리는_버리고_짧은_끊김은_잇는다():
    blip = [0.9] * 3  # 96ms < 최소 발화 250ms
    dip = [0.1] * 2  # 64ms < 최소 침묵 100ms
    probs = [0.1] * 10 + blip + [0.1] * 10 + [0.9] * 20 + dip + [0.9] * 20 + [0.1] * 10
    total = len(probs) * a.VAD_WINDOW

    segments = a.speech_segments(probs, total)
    assert len(segments) == 1  # blip 제거, dip 은 하나로 이어짐


def test_후처리_종료는_음의_임계_아래로_떨어져야_한다():
    # 0.4 는 시작 임계(0.5) 아래지만 종료 임계(0.35) 위 — 발화가 끊기지 않는다
    probs = [0.9] * 20 + [0.4] * 20 + [0.9] * 20
    total = len(probs) * a.VAD_WINDOW
    assert len(a.speech_segments(probs, total)) == 1


# --- 음절 ---------------------------------------------------------------------

@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("엔티티를 관리하는 캐시입니다", 13),
        ("1차 캐시", 4),  # 숫자 1 + 한글 3
        ("JPA persistence", 5),  # JPA→1(모음군 A), persistence→4
        ("", 0),
        ("... !!!", 0),
    ],
)
def test_음절_세기(text, expected):
    assert a.count_syllables(text) == expected


# --- 산식·태그 ----------------------------------------------------------------

def metrics(*, phonation=60.0, syllables=300, pause=0.0, long_pauses=0) -> a.DeliveryMetrics:
    return a.DeliveryMetrics(
        duration_s=phonation + pause, phonation_s=phonation, syllables=syllables,
        pause_s=pause, long_pause_count=long_pauses,
    )


def test_적정_속도에_침묵_없으면_만점():
    assert a.score_of(metrics()) == 100  # 5.0 음절/초
    assert a.tags_of(metrics()) == []


@pytest.mark.parametrize(
    ("syllables", "expected_score", "expected_tag"),
    [
        (450, 80, a.TAG_FAST),  # 7.5/s — 범위 밖 1.0 → -20
        (600, 60, a.TAG_FAST),  # 10/s — 범위 밖 3.5 → 상한 -40
        (180, 80, a.TAG_SLOW),  # 3.0/s — 범위 밖 1.0 → -20
    ],
)
def test_속도_이탈_감점과_태그(syllables, expected_score, expected_tag):
    m = metrics(syllables=syllables)
    assert a.score_of(m) == expected_score
    assert [t.tag for t in a.tags_of(m)] == [expected_tag]


def test_침묵_비율_감점은_자유_구간_위로만():
    free = metrics(pause=10.0)  # 10/70 = 0.14 < 0.15
    assert a.score_of(free) == 100
    heavy = metrics(pause=60.0)  # 0.5 ≥ 0.45 → 상한 -40
    assert a.score_of(heavy) == 60


def test_긴_침묵_빈도_감점과_태그():
    # 답변 60s 발성 + 20s 침묵 = 80s, 긴 침묵 4회 → 3.0/min → -15, 침묵 비율 0.25 → -13.3
    m = metrics(pause=20.0, long_pauses=4)
    assert a.score_of(m) == 72
    assert a.tags_of(m) == [a.WeaknessTagCount(tag=a.TAG_FREQUENT_SILENCE, count=4)]


def test_잦은_침묵_태그는_최소_횟수를_요구한다():
    # 분당 2회 이상이지만 총 2회 — 짧은 세션의 우연으로 보고 태그 없음
    m = metrics(phonation=30.0, syllables=150, pause=10.0, long_pauses=2)
    assert m.long_pauses_per_minute >= a.LONG_PAUSE_TAG_PER_MIN
    assert a.tags_of(m) == []


def test_점수는_0_아래로_내려가지_않는다():
    m = metrics(syllables=1200, pause=100.0, long_pauses=30)
    assert a.score_of(m) == 0


def test_측정_불가는_점수와_태그가_없다():
    assert a.score_of(metrics(phonation=5.0, syllables=100)) is None
    assert a.tags_of(metrics(phonation=5.0, syllables=100)) == []
    assert a.score_of(metrics(syllables=10)) is None
