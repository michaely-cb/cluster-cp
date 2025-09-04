import json
from pathlib import Path

import pytest

test_dir = Path(__file__).parent


# Copied from /cb/system_data/clusters/multibox18
@pytest.fixture(scope="session")
def net_cfg_16_system() -> dict:
  return json.loads((test_dir / "net.json").read_text())
