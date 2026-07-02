"""Point config at a throwaway directory before any module import, so tests
never touch the repo's data/ or logs/ directories."""
import os
import sys
import tempfile
from pathlib import Path

_tmp = Path(tempfile.mkdtemp(prefix="far-collector-tests-"))
os.environ.setdefault("FAR_DATA_DIR", str(_tmp / "data"))
os.environ.setdefault("FAR_LOG_DIR", str(_tmp / "logs"))

sys.path.insert(0, str(Path(__file__).parent.parent))
