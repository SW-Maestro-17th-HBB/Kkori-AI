"""텍스트 추출 — S3/MinIO 다운로드 + PyMuPDF (PRD §2.1).

FULL 파이프라인 2단계(TEXT_EXTRACTING)의 재료:
- S3 에서 원본 PDF 를 내려받아 전체 페이지의 텍스트를 추출해 **반환만** 한다.
  원문 텍스트는 어디에도 저장하지 않는다 (§2.1 — 원문 미저장).
- 빈 추출(공백 제거 후 길이 0) 판정은 `is_empty_text` 로 제공하고,
  그때 FAILED 로 종결하는 것은 호출자(파이프라인) 책임이다 (§2.1 — 이미지-only PDF).
- 손상 PDF·다운로드 실패는 예외를 그대로 전파한다 — 재시도/FAILED 판단은 호출자 몫.
"""

from __future__ import annotations

import fitz  # PyMuPDF

from src.config import Settings


def build_s3_client(settings: Settings):
    """S3/MinIO 클라이언트. 자격증명이 설정에 없으면 boto3 기본 체인(IAM Role 등)에 맡긴다."""
    import boto3

    kwargs: dict = {"region_name": settings.s3_region}
    if settings.s3_endpoint:
        kwargs["endpoint_url"] = settings.s3_endpoint
    if settings.s3_access_key and settings.s3_secret_key:
        kwargs["aws_access_key_id"] = settings.s3_access_key
        kwargs["aws_secret_access_key"] = settings.s3_secret_key
    return boto3.client("s3", **kwargs)


def download_pdf(s3_client, bucket: str, object_key: str) -> bytes:
    """S3 객체를 통째로 내려받는다 (업로드 검증이 10MB 를 제한하므로 메모리 적재 허용)."""
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    return response["Body"].read()


def extract_text(pdf_bytes: bytes) -> str:
    """PDF 전체 페이지의 텍스트를 추출해 합쳐 반환한다.

    PyMuPDF 는 텍스트 레이어만 읽는다 — 이미지-only(스캔) PDF 는 빈 문자열이 나온다.
    손상 PDF 는 예외를 그대로 전파한다.
    """
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        return "\n".join(page.get_text() for page in doc)


def is_empty_text(text: str) -> bool:
    """공백 제거 후 길이 0 이면 빈 추출 (§2.1 — 호출자가 FAILED 로 종결)."""
    return len(text.strip()) == 0
