"""pytest가 worker/ 를 import 경로에 넣도록 한다.

Docker에서는 worker/src/ 가 /app/src/ 로 복사돼 `src` 패키지가 되지만(Dockerfile 참고),
로컬에서 `pytest worker` 로 돌릴 때도 `import src.*` 가 되도록 worker/ 를 sys.path 에 추가한다.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
