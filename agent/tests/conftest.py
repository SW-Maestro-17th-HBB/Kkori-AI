from pathlib import Path

from dotenv import load_dotenv

# LLM 스모크 테스트가 로컬에서 자격증명을 읽도록 agent/.env 로드 (없으면 no-op → CI에서는 skip)
load_dotenv(Path(__file__).resolve().parents[1] / ".env")
