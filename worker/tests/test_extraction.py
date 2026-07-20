"""텍스트 추출 테스트 (§2.1) — PDF 픽스처는 테스트가 직접 생성한다."""

import uuid

import fitz  # PyMuPDF
import pytest

from src.config import Settings
from src.analysis.extraction import build_s3_client, download_pdf, extract_text, is_empty_text

MINIO_SETTINGS = Settings(
    s3_endpoint_url="http://localhost:9000",
    s3_access_key="kkori",
    s3_secret_key="kkori1234",
)
BUCKET = "kkori-resumes"


def _make_pdf(texts: list[str]) -> bytes:
    """페이지별 텍스트를 가진 PDF 를 만들어 bytes 로 반환 (텍스트 없으면 빈 페이지).

    PyMuPDF 기본 폰트(helv)는 한글 글리프가 없어 픽스처 텍스트는 ASCII 로 쓴다 —
    추출 로직 검증에 언어는 무관하다.
    """
    doc = fitz.open()
    for text in texts:
        page = doc.new_page()
        if text:
            page.insert_text((72, 72), text)
    data = doc.tobytes()
    doc.close()
    return data


def _minio_available() -> bool:
    try:
        client = build_s3_client(MINIO_SETTINGS)
        client.head_bucket(Bucket=BUCKET)
        return True
    except Exception:
        return False


requires_minio = pytest.mark.skipif(
    not _minio_available(), reason="로컬 MinIO(9000) 없음 — S3 테스트 건너뜀"
)


def test_텍스트_추출_전체페이지():
    pdf = _make_pdf(["page one content", "page two content"])
    text = extract_text(pdf)
    assert "page one content" in text
    assert "page two content" in text
    assert not is_empty_text(text)


def test_빈_페이지는_빈_추출():
    pdf = _make_pdf(["", ""])  # 텍스트 레이어 없는 페이지 (이미지-only PDF 와 동일 결과)
    text = extract_text(pdf)
    assert is_empty_text(text)


def test_손상_PDF는_예외_전파():
    with pytest.raises(Exception):
        extract_text("PDF 가 아닌 바이트".encode("utf-8"))


def test_is_empty_text_공백만_있어도_빈것():
    assert is_empty_text("")
    assert is_empty_text("  \n\t ")
    assert not is_empty_text(" 내용 ")


@requires_minio
def test_S3_업로드_다운로드_왕복():
    client = build_s3_client(MINIO_SETTINGS)
    key = f"test/{uuid.uuid4()}.pdf"
    pdf = _make_pdf(["roundtrip test"])
    client.put_object(Bucket=BUCKET, Key=key, Body=pdf)
    try:
        downloaded = download_pdf(client, BUCKET, key)
        assert downloaded == pdf
        assert "roundtrip test" in extract_text(downloaded)
    finally:
        client.delete_object(Bucket=BUCKET, Key=key)


@requires_minio
def test_없는_객체는_예외():
    client = build_s3_client(MINIO_SETTINGS)
    with pytest.raises(Exception):
        download_pdf(client, BUCKET, "없는/객체.pdf")
