#!/usr/bin/env bash

# NOTE: .sh extension is dropped because this file will live in /etc/cron.*
# and cron will not run it if it has a .sh extension.

# this is necessary since crontab won't have the env var
export PATH="${PATH}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"

set -eo pipefail

export script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source ${script_dir}/../common.sh
get_utc_current_time

mkdir -p /n0/cluster-mgmt/job-archive

jobs=$(kubectl get wsjobs.jobs.cerebras.com -A -o json | jq --arg s $(date +%Y-%m-%dT%H:%M:%SZ --date="-1 day -6 hours") -r '.items | map(select(.status.completionTime | . >= $s + "z"))')
num_jobs=$(echo $jobs | jq length)

job_idx=0

while [ $job_idx -lt $num_jobs ]; do
    filename=/n0/cluster-mgmt/job-archive/$(echo $jobs | jq -r .[$job_idx].metadata.name).json
    echo $jobs  | jq .[$job_idx] > $filename
    job_idx=$(($job_idx + 1))
done

# clean up
find /n0/cluster-mgmt/job-archive -name '*.json' -type f -mtime +365 -delete
