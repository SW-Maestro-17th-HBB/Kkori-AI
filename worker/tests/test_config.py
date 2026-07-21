"""설정 로드·오버라이드 테스트."""

from src.config import Settings, get_settings


def test_기본값_로드():
    s = Settings()
    assert s.redis_url.startswith("redis://")
    assert s.embedding_dim == 1024
    assert s.delivery_count_threshold == 3
    assert s.chunk_target_tokens == 512


def test_환경변수_오버라이드(monkeypatch):
    monkeypatch.setenv("KKORI_WORKER_DELIVERY_COUNT_THRESHOLD", "5")
    monkeypatch.setenv("KKORI_WORKER_BEDROCK_REGION", "ap-northeast-2")
    s = Settings()
    assert s.delivery_count_threshold == 5
    assert s.bedrock_region == "ap-northeast-2"


def test_consumer_name_비면_hostname():
    s = Settings()
    assert s.resolved_consumer_name  # 비어 있지 않다


def test_s3_endpoint_빈값은_None(monkeypatch):
    monkeypatch.setenv("KKORI_WORKER_S3_ENDPOINT_URL", "")
    s = Settings()
    assert s.s3_endpoint is None


def test_get_settings_캐시_동일_인스턴스():
    assert get_settings() is get_settings()
