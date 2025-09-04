#
# The wrapper script to initiate memoryX node qualification test on a memoryX node.
# The output of the script is written to a file located in the current workdir.
#

set -o pipefail

TEST_SCRIPT_FILE="memx_simulate_training_benchmark.sh"

/bin/bash ${TEST_SCRIPT_FILE} 2>&1 | tee ${MY_POD_NAME}.out