"""전달력(음성) 분석 — 결정적 규칙, LLM 없음.

입력은 세션 녹음(OGG/Opus, **지원자 음성만 담긴 파일** — 세션 도메인 담당자 확인 2026-09-04)
과 대본의 지원자 발화 텍스트, 출력은 `delivery_score`(0~100 정수, 측정 불가면 None)·음성
약점 태그·지표다.

지표·임계값·산식의 정의 원천은 `worker/docs/requirements/report-evaluation/delivery-score.md`
— 값을 바꾸면 그 문서를 먼저 고친다. 같은 녹음·대본에서는 항상 같은 결과가 나온다
(백엔드 PRD "음성 분석은 결정적 산식 — 재분석의 실익이 없다"의 전제).

흐름:
1. 디코드·리샘플 — 파일을 블록으로 읽어(30분 녹음을 통째로 올리면 수백 MB) 채널 평균으로
   모노를 만들고, FIR 저역 필터 후 솎아내 16kHz 로 맞춘다 (Opus 는 항상 48kHz 디코딩)
2. 발화 확률 — Silero VAD(ONNX, onnxruntime) 에 512샘플(32ms) 청크를 순서대로 넣는다
3. 발화 구간 — Silero 공식 `get_speech_timestamps` 의 후처리를 그대로 옮겨 구간을 만든다
   (시작 0.5·종료 0.35·최소 발화 250ms·최소 침묵 100ms·패딩 30ms)
4. 침묵 — 발화 사이 공백 중 TURN_GAP_S 이하만 "답변 중 침묵"으로 센다. 그보다 긴 공백은
   면접관이 말하는 차례로 보고 제외한다(파일에 면접관 음성이 없어 구분 근거가 공백 길이뿐)
5. 말 속도 = 대본 음절 수 ÷ 발성 시간 (음절/초). 음절은 STT 대본에서 세고, 시간은
   오디오에서 잰다 — 오디오만으로 음절을 세는 것(피크 검출)보다 안정적이다
6. 점수 = 100 − 속도 감점 − 침묵 비율 감점 − 긴 침묵 빈도 감점, 태그는 같은 임계로 판정

Silero VAD: https://github.com/snakers4/silero-vad v6.2.1, MIT — `models/silero_vad.onnx`
(sha256 1a153a22…). 공식 파이썬 패키지는 torch 를 끌고 오므로 쓰지 않고 모델 파일만 둔다.
"""

from __future__ import annotations

import io
import math
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import numpy as np
import soundfile as sf

from src.contract import WeaknessTagCount

# ---------------------------------------------------------------- 어휘집 (음성 태그)

TAG_FAST = "말 속도 빠름"
TAG_SLOW = "말 속도 느림"
TAG_FREQUENT_SILENCE = "잦은 침묵"
# "군말 잦음" 은 어휘집(evaluation-criteria §2.2)에 있으나 이번 범위에서 판정하지 않는다 —
# STT 대본은 군말을 대부분 걷어내고, 오디오만으로 군말을 가려내는 결정적 규칙이 없다.
VOICE_WEAKNESS_TAGS: tuple[str, ...] = (TAG_FAST, TAG_SLOW, TAG_FREQUENT_SILENCE, "군말 잦음")

# ---------------------------------------------------------------- 임계값 (잠정 — delivery-score.md §3)

TURN_GAP_S = 10.0  # 발화 사이 공백이 이보다 길면 면접관 차례로 보고 침묵에서 제외
LONG_PAUSE_S = 2.0  # 긴 침묵 판정
RATE_SLOW = 4.0  # 음절/초 — 이 미만이면 느림
RATE_FAST = 6.5  # 음절/초 — 이 초과면 빠름
RATE_PENALTY_PER_UNIT = 20.0  # 적정 범위 밖 1음절/초당 감점
RATE_PENALTY_CAP = 40.0
PAUSE_RATIO_FREE = 0.15  # 침묵 비율 이하는 감점 없음
PAUSE_RATIO_MAX = 0.45  # 이 이상이면 침묵 감점 상한
PAUSE_PENALTY_CAP = 40.0
LONG_PAUSE_PENALTY_PER_MIN = 5.0  # 분당 긴 침묵 1회당 감점
LONG_PAUSE_PENALTY_CAP = 20.0
LONG_PAUSE_TAG_PER_MIN = 2.0  # 분당 이 이상이면 "잦은 침묵"
LONG_PAUSE_TAG_MIN_COUNT = 3  # 짧은 세션의 우연을 거르는 최소 횟수

MIN_PHONATION_S = 10.0  # 발성 시간이 이보다 짧으면 측정 불가
MIN_SYLLABLES = 30  # 음절이 이보다 적으면 측정 불가

# Silero VAD 후처리 — 공식 get_speech_timestamps 기본값 (v6.2.1)
VAD_SAMPLE_RATE = 16_000
VAD_WINDOW = 512  # 32ms
VAD_CONTEXT = 64  # 직전 청크 꼬리를 앞에 붙인다(모델 입력 = 64 + 512)
VAD_THRESHOLD = 0.5
VAD_NEG_THRESHOLD = 0.35  # threshold - 0.15
VAD_MIN_SPEECH_MS = 250
VAD_MIN_SILENCE_MS = 100
VAD_SPEECH_PAD_MS = 30

MODEL_PATH = Path(__file__).parent / "models" / "silero_vad.onnx"

Segment = tuple[float, float]  # (시작 초, 끝 초)


# ---------------------------------------------------------------- 지표·결과

@dataclass(frozen=True)
class DeliveryMetrics:
    """산식 입력이 되는 지표 — 로그·검증·보정에 쓴다."""

    duration_s: float  # 녹음 길이
    phonation_s: float  # 발성 시간(발화 구간 합)
    syllables: int  # 대본 지원자 발화 음절 수
    pause_s: float  # 답변 중 침묵 합 (TURN_GAP_S 이하 공백)
    long_pause_count: int  # LONG_PAUSE_S 이상 침묵 횟수

    @property
    def articulation_rate(self) -> float | None:
        """말 속도(음절/초). 발성 시간이 0이면 None."""
        if self.phonation_s <= 0:
            return None
        return self.syllables / self.phonation_s

    @property
    def answering_s(self) -> float:
        """답변 시간 = 발성 + 답변 중 침묵 (면접관 차례 제외)."""
        return self.phonation_s + self.pause_s

    @property
    def pause_ratio(self) -> float:
        return self.pause_s / self.answering_s if self.answering_s > 0 else 0.0

    @property
    def long_pauses_per_minute(self) -> float:
        if self.answering_s <= 0:
            return 0.0
        return self.long_pause_count / (self.answering_s / 60.0)

    @property
    def measurable(self) -> bool:
        return self.phonation_s >= MIN_PHONATION_S and self.syllables >= MIN_SYLLABLES


@dataclass(frozen=True)
class DeliveryResult:
    score: int | None  # None = 측정 불가(발화 부족) — delivery_score NULL 로 저장
    tags: list[WeaknessTagCount] = field(default_factory=list)
    metrics: DeliveryMetrics | None = None


# ---------------------------------------------------------------- 1. 디코드·리샘플

def _lowpass_taps(decimation: int, taps: int = 63) -> np.ndarray:
    """솎아내기 전 저역 필터(윈도우 sinc, Hamming) — 차단 주파수는 새 나이퀴스트의 0.875배."""
    cutoff = 0.5 / decimation * 0.875
    n = np.arange(taps) - (taps - 1) / 2
    h = 2 * cutoff * np.sinc(2 * cutoff * n) * np.hamming(taps)
    return h / h.sum()


class _Resampler:
    """블록 단위 모노 변환 + 저역 필터 + 정수배 솎아내기. 블록 경계는 carry 로 잇는다."""

    def __init__(self, source_rate: int):
        if source_rate % VAD_SAMPLE_RATE:
            raise ValueError(f"지원하지 않는 샘플레이트 {source_rate} — 16kHz 의 정수배만 받는다")
        self.step = source_rate // VAD_SAMPLE_RATE
        self._taps = _lowpass_taps(self.step) if self.step > 1 else None
        self._filter_carry = np.zeros(len(self._taps) - 1) if self._taps is not None else None
        self._phase = 0  # 다음 블록에서 처음 취할 샘플의 오프셋

    def push(self, block: np.ndarray) -> np.ndarray:
        mono = block.astype(np.float64).mean(axis=1)
        if self.step == 1:
            return mono.astype(np.float32)
        padded = np.concatenate([self._filter_carry, mono])
        self._filter_carry = padded[-(len(self._taps) - 1):]
        filtered = np.convolve(padded, self._taps, mode="valid")  # len == len(mono)
        taken = filtered[self._phase::self.step]
        self._phase = (self._phase - len(filtered)) % self.step
        return taken.astype(np.float32)


def resample_to_16k(file, *, block_seconds: int = 20):
    """soundfile 로 블록을 읽어 16kHz 모노 float32 배열을 차례로 낸다 (제너레이터)."""
    with sf.SoundFile(file) as snd:
        resampler = _Resampler(snd.samplerate)
        block_size = snd.samplerate * block_seconds
        while True:
            block = snd.read(block_size, dtype="float32", always_2d=True)
            if len(block) == 0:
                break
            yield resampler.push(block)


# ---------------------------------------------------------------- 2. 발화 확률 (Silero VAD)

class SileroVad:
    """Silero VAD ONNX 모델 — 공식 OnnxWrapper 의 numpy 이식(단일 배치, 16kHz 고정).

    세션은 재사용하고(스레드 안전) 순환 상태(state·context)는 호출마다 새로 만든다.
    """

    def __init__(self, model_path: Path = MODEL_PATH):
        import onnxruntime as ort

        opts = ort.SessionOptions()
        opts.inter_op_num_threads = 1
        opts.intra_op_num_threads = 1
        self._session = ort.InferenceSession(
            str(model_path), sess_options=opts, providers=["CPUExecutionProvider"]
        )

    def probabilities(self, chunks) -> list[float]:
        """16kHz 모노 블록들을 512샘플 청크로 잘라 순서대로 발화 확률을 낸다. 끝 자투리는 0 패딩."""
        state = np.zeros((2, 1, 128), dtype=np.float32)
        context = np.zeros((1, VAD_CONTEXT), dtype=np.float32)
        sr = np.array(VAD_SAMPLE_RATE, dtype=np.int64)
        carry = np.empty(0, dtype=np.float32)
        probs: list[float] = []

        def run(chunk: np.ndarray) -> None:
            nonlocal state, context
            x = np.concatenate([context, chunk[None, :]], axis=1)
            out, state = self._session.run(None, {"input": x, "state": state, "sr": sr})
            context = x[:, -VAD_CONTEXT:]
            probs.append(float(out[0, 0]))

        for block in chunks:
            data = np.concatenate([carry, block]) if len(carry) else block
            usable = (len(data) // VAD_WINDOW) * VAD_WINDOW
            for start in range(0, usable, VAD_WINDOW):
                run(data[start:start + VAD_WINDOW])
            carry = data[usable:]
        if len(carry):
            run(np.pad(carry, (0, VAD_WINDOW - len(carry))))
        return probs


@lru_cache
def default_vad() -> SileroVad:
    return SileroVad()


# ---------------------------------------------------------------- 3. 발화 구간 (공식 후처리 이식)

def speech_segments(probs: list[float], total_samples: int) -> list[Segment]:
    """청크별 확률 → 발화 구간(초). Silero get_speech_timestamps(max_speech=inf) 와 같은 규칙."""
    sr = VAD_SAMPLE_RATE
    min_speech = sr * VAD_MIN_SPEECH_MS / 1000
    min_silence = sr * VAD_MIN_SILENCE_MS / 1000
    pad = sr * VAD_SPEECH_PAD_MS / 1000

    speeches: list[list[int]] = []
    triggered = False
    start = 0
    temp_end = 0
    for i, prob in enumerate(probs):
        cur = VAD_WINDOW * i
        if prob >= VAD_THRESHOLD and temp_end:
            temp_end = 0
        if prob >= VAD_THRESHOLD and not triggered:
            triggered = True
            start = cur
            continue
        if prob < VAD_NEG_THRESHOLD and triggered:
            if not temp_end:
                temp_end = cur
            if cur - temp_end < min_silence:
                continue
            if temp_end - start > min_speech:
                speeches.append([start, temp_end])
            temp_end = 0
            triggered = False
    if triggered and total_samples - start > min_speech:
        speeches.append([start, total_samples])

    for i, speech in enumerate(speeches):
        if i == 0:
            speech[0] = int(max(0, speech[0] - pad))
        if i != len(speeches) - 1:
            silence = speeches[i + 1][0] - speech[1]
            if silence < 2 * pad:
                speech[1] += int(silence // 2)
                speeches[i + 1][0] = int(max(0, speeches[i + 1][0] - silence // 2))
            else:
                speech[1] = int(min(total_samples, speech[1] + pad))
                speeches[i + 1][0] = int(max(0, speeches[i + 1][0] - pad))
        else:
            speech[1] = int(min(total_samples, speech[1] + pad))
    return [(s / sr, e / sr) for s, e in speeches]


# ---------------------------------------------------------------- 4. 침묵

def answer_pauses(segments: list[Segment], *, turn_gap_s: float = TURN_GAP_S) -> list[float]:
    """발화 사이 공백 중 답변 중 침묵으로 볼 것들(초).

    turn_gap_s 를 넘는 공백은 면접관이 질문하는 차례(또는 그 뒤 생각하는 시간)로 보고 뺀다.
    파일에 면접관 음성이 없어 구분 근거는 공백 길이뿐이다 — 긴 답변 중 침묵이 이 상한을
    넘으면 놓치지만, 면접관 차례를 침묵으로 잘못 세는 것보다 낫다(잠정 규칙).
    """
    return [
        gap
        for (_, prev_end), (next_start, _) in zip(segments, segments[1:])
        if 0 < (gap := next_start - prev_end) <= turn_gap_s
    ]


# ---------------------------------------------------------------- 5. 음절

_HANGUL = re.compile(r"[가-힣]")
_DIGIT = re.compile(r"\d")
_LATIN_WORD = re.compile(r"[A-Za-z]+")
_LATIN_VOWEL_GROUP = re.compile(r"[aeiouyAEIOUY]+")


def count_syllables(text: str) -> int:
    """대본 텍스트의 음절 수 근사 — 한글 1글자 1음절, 숫자 1자리 1음절, 영단어는 모음군 수.

    STT 대본은 대부분 한글이라 근사 오차는 영어 용어 몇 개 수준이다.
    """
    hangul = len(_HANGUL.findall(text))
    digits = len(_DIGIT.findall(text))
    latin = sum(
        max(1, len(_LATIN_VOWEL_GROUP.findall(word))) for word in _LATIN_WORD.findall(text)
    )
    return hangul + digits + latin


# ---------------------------------------------------------------- 6. 산식·태그

def _round_half_up(value: float) -> int:
    return math.floor(value + 0.5)


def score_of(metrics: DeliveryMetrics) -> int | None:
    """delivery_score — 측정 불가면 None. 산식은 delivery-score.md §4."""
    if not metrics.measurable:
        return None
    penalty = 0.0
    rate = metrics.articulation_rate or 0.0
    outside = max(0.0, RATE_SLOW - rate, rate - RATE_FAST)
    penalty += min(RATE_PENALTY_CAP, outside * RATE_PENALTY_PER_UNIT)
    excess_ratio = max(0.0, metrics.pause_ratio - PAUSE_RATIO_FREE)
    penalty += min(
        PAUSE_PENALTY_CAP,
        excess_ratio / (PAUSE_RATIO_MAX - PAUSE_RATIO_FREE) * PAUSE_PENALTY_CAP,
    )
    penalty += min(
        LONG_PAUSE_PENALTY_CAP, metrics.long_pauses_per_minute * LONG_PAUSE_PENALTY_PER_MIN
    )
    return max(0, min(100, _round_half_up(100.0 - penalty)))


def tags_of(metrics: DeliveryMetrics) -> list[WeaknessTagCount]:
    """음성 약점 태그 — 속도 태그는 세션 단위 판정 1회(count=1), 잦은 침묵은 긴 침묵 횟수."""
    if not metrics.measurable:
        return []
    tags: list[WeaknessTagCount] = []
    rate = metrics.articulation_rate or 0.0
    if rate > RATE_FAST:
        tags.append(WeaknessTagCount(tag=TAG_FAST, count=1))
    elif rate < RATE_SLOW:
        tags.append(WeaknessTagCount(tag=TAG_SLOW, count=1))
    if (
        metrics.long_pauses_per_minute >= LONG_PAUSE_TAG_PER_MIN
        and metrics.long_pause_count >= LONG_PAUSE_TAG_MIN_COUNT
    ):
        tags.append(WeaknessTagCount(tag=TAG_FREQUENT_SILENCE, count=metrics.long_pause_count))
    return tags


# ---------------------------------------------------------------- 조립

def measure(segments: list[Segment], duration_s: float, syllables: int) -> DeliveryMetrics:
    """발화 구간 + 음절 수 → 지표. 순수 함수라 구간 목록만으로 검증한다."""
    pauses = answer_pauses(segments)
    return DeliveryMetrics(
        duration_s=duration_s,
        phonation_s=sum(e - s for s, e in segments),
        syllables=syllables,
        pause_s=sum(pauses),
        long_pause_count=sum(1 for p in pauses if p >= LONG_PAUSE_S),
    )


def detect_speech(file, vad: SileroVad | None = None) -> tuple[list[Segment], float]:
    """녹음 파일(경로 또는 파일 객체) → (발화 구간, 길이 초)."""
    vad = vad or default_vad()
    total = 0

    def counted():
        nonlocal total
        for block in resample_to_16k(file):
            total += len(block)
            yield block

    probs = vad.probabilities(counted())
    return speech_segments(probs, total), total / VAD_SAMPLE_RATE


def analyze(recording: bytes, candidate_text: str, vad: SileroVad | None = None) -> DeliveryResult:
    """녹음 바이트 + 지원자 발화 텍스트 → 전달력 결과. blocking — 호출자가 스레드로 넘긴다."""
    segments, duration_s = detect_speech(io.BytesIO(recording), vad)
    metrics = measure(segments, duration_s, count_syllables(candidate_text))
    return DeliveryResult(score=score_of(metrics), tags=tags_of(metrics), metrics=metrics)
