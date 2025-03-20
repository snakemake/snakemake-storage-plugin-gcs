import pytest
from pathlib import Path

pytest.main([(Path(__file__).parent / "tests.py").as_posix()])
