import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from logger import configure_logging  # noqa: E402

# Entry-point responsibility: point the logger singleton at log/test_<date>.log
# so test runs do not write into the production data_<date>.log.
configure_logging(data_path=str(Path(__file__).resolve().parents[1] / "data" / "test"))
