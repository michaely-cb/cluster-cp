import json
import logging

import pathlib

from deployment_manager.tools.pb1.ansible_task import get_failure_context

logger = logging.getLogger(__name__)

ASSETS_PATH = pathlib.Path(__file__).parent.parent / "tests" / "assets" / "files"


def test_get_failure_context():
    doc = json.loads((ASSETS_PATH / "ansible_stdout.json").read_text())
    failure_context = get_failure_context(doc)
    assert len(failure_context["localhost"]) == 2
