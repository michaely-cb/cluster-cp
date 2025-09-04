#!/usr/bin/env python3

import os
import sys

# This is only relevant to user sidecar environments, where cluster mgmt should tell
# Python to respect the additional mounted volumes so dependencies could be looked
# up from those volumes. Historically, we used to append PYTHONPATH_ADDON to
# PYTHONPATH, but the mounted dependencies would undesirably take precedence to
# what we have in sys path. Here, we append PYTHONPATH_ADDON to the end of sys path,
# so the entire dependency resolution chain is respected before attempting to
# resolve the dependencies from the mounted volumes.
if "PYTHONPATH_ADDON" in os.environ:
    sys.path.extend(os.environ["PYTHONPATH_ADDON"].split(":"))
