"""LLM 생성 팩토리 — 프로바이더 토글(전환기 한정, Bedrock 안정화 후 inference 분기 제거 예정).

main·테스트·preview 스크립트가 같은 경로로 LLM을 만들도록 분리한 모듈.
환경변수는 각 진입점의 load_dotenv() 이후인 호출 시점에 읽는다.
"""

from __future__ import annotations

import os

from livekit.agents import inference, llm
from livekit.plugins import aws

from src.config import BEDROCK_REGION, DEFAULT_LLM_PROVIDER, LLM_PROVIDER_ENV


def build_llm(inference_model: str, bedrock_model: str) -> llm.LLM:
    """토글에 따라 역할별 모델 쌍 중 하나로 LLM을 만든다.

    Bedrock 자격증명은 boto3 기본 체인(환경변수 AWS_ACCESS_KEY_ID/SECRET)에 맡긴다.
    """
    provider = os.getenv(LLM_PROVIDER_ENV, DEFAULT_LLM_PROVIDER)
    if provider == "bedrock":
        return aws.LLM(model=bedrock_model, region=BEDROCK_REGION)
    if provider == "inference":
        return inference.LLM(model=inference_model)
    raise ValueError(f"알 수 없는 {LLM_PROVIDER_ENV}: {provider}")
