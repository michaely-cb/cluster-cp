#!/bin/bash

#
# Call cbda interface to record the start of a given job. The run_id for the job
# is printed out to $system_run_id file.
#

SCRIPT_PATH=$(dirname "$0")
cd "$SCRIPT_PATH"

function main() {
    system=$1
    db=$2

    if [ "$db" == "prod" ]; then
        db=""
    else
        db="-$db"
    fi

    if [ ! -e "venv" ]; then
        python3 -m venv venv
    fi
    source venv/bin/activate
    pip3 install -r requirements.txt
    rm -f "$system"_run_id
    SYSTEM=$system DB=$db PYTHONPATH=$PYTHONPATH:. python3 -c "from cbda_interface import CbdaBaseInterface, JobName, JobStatus; import os; \
        CbdaBaseInterface.POSTGRES_SQL_DB_HOST = f\"cbda-postgres{os.environ['DB']}.cerebras.aws\"; \
        cbda = CbdaBaseInterface(os.environ['SYSTEM'], JobName.CLUSTER_DEPLOY); cbda.start_run(); print(f\"{cbda.system_bring_up_run_id}\");" > "$system"_run_id
    deactivate
}

if [[ "$#" -lt 2 ]] || ([ "$2" != "prod" ] && [ "$2" != "dev" ]); then
  echo "Usage: $0 <system> <prod/dev>"
  exit 1
fi

main $1 $2
