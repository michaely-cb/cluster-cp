#!/bin/bash

#
# Call cbda interface to record the end of a given job run_id.
#

SCRIPT_PATH=$(dirname "$0")
cd "$SCRIPT_PATH"

function main() {
    system=$1
    db=$2
    run_id=$3
    status=$4

    if [ "$db" == "prod" ]; then
        db=""
    else
        db="-$db"
    fi

    if [ ! -f "venv/bin/activate" ]; then
        python3 -m venv venv
    fi
    source venv/bin/activate
    pip3 install -r requirements.txt
    if [ "$status" == "pass" ]; then
        SYSTEM=$system DB=$db RUN_ID=$run_id PYTHONPATH=$PYTHONPATH:. python3 -c "from cbda_interface import CbdaBaseInterface, JobName, JobStatus; import os; \
            CbdaBaseInterface.POSTGRES_SQL_DB_HOST = f\"cbda-postgres{os.environ['DB']}.cerebras.aws\"; \
            cbda = CbdaBaseInterface(os.environ['SYSTEM'], JobName.CLUSTER_DEPLOY); cbda.system_bring_up_run_id = int(os.environ['RUN_ID']); cbda.end_run(JobStatus.PASS);"
    else
        SYSTEM=$system DB=$db RUN_ID=$run_id PYTHONPATH=$PYTHONPATH:. python3 -c "from cbda_interface import CbdaBaseInterface, JobName, JobStatus; import os; \
            CbdaBaseInterface.POSTGRES_SQL_DB_HOST = f\"cbda-postgres{os.environ['DB']}.cerebras.aws\"; \
            cbda = CbdaBaseInterface(os.environ['SYSTEM'], JobName.CLUSTER_DEPLOY); cbda.system_bring_up_run_id = int(os.environ['RUN_ID']); cbda.end_run(JobStatus.FAIL);"
    fi
    deactivate
}

if [[ "$#" -lt 4 ]] || ([ "$2" != "prod" ] && [ "$2" != "dev" ]) || ([ "$4" != "pass" ] && [ "$4" != "fail" ]); then
    echo "Usage: $0 <system> <prod/dev> <job_run_id> <pass/fail>"
    exit 1
fi

main $1 $2 $3 $4
