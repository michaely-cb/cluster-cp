#!/usr/bin/env bash

set -e

function usage() {
    echo "Usage: $0 [--help] [--elf] [--streamers]"
}

function die() {
    printf '%s\n' "$1" >&2
    exit 1
}

ELF=""
STREAMERS=1

function get_inputs() {
  while :; do
    case $1 in
      -h|-\?|--help)
        usage
        exit
        ;;
      --elf)
        if [ "$2" ]; then
          ELF=$2
          shift 2
        else
          die 'ERROR: "--elf" requires a non-empty argument'
        fi
        ;;
      --elf=*)
        ELF="${1#*=}"
        shift
        ;;
      --streamers)
        if [ "$2" ]; then
          STREAMERS=$2
          shift 2
        else
          die 'ERROR: "--egress-amplify" requires a non-empty integer argument'
        fi
        ;;
      --streamers=*)
        STREAMERS="${1#*=}"
        shift
        ;;
      --)
        shift
        ;;
      -?*)
        printf 'WARN: Unknown option (ignored): %s\n' "$1" >&2
        shift
        ;;
      *)
        break
        ;;
    esac
  done
}

get_inputs "$@"

# It appears that workingDir is handled different in 1.6 and master by the cluster-server/job-operator.
# We check the current workingDir. If it is not role specific, we create the role-specific directory,
# and cd into it before running the rest of the commands.
if [[ ${PWD} == *chief-${REPLICA_ID} ]]; then
  echo "Current workingDir ${PWD}"
else
  mkdir -p ${PWD}/chief-${REPLICA_ID}
  cd ${PWD}/chief-${REPLICA_ID}
  echo "Change workingDir to ${PWD}"
fi

CMADDR=$(less /opt/cerebras/cfg/br_cfg_json | python3 -c "import sys, json; print(json.load(sys.stdin)['cs_nodes'][${REPLICA_ID}]['cmaddr'])")
echo "Programming ${CMADDR}"
DEFAULT_ELF="${PWD}/default.elf"

config_api  -s  ${CMADDR} -c version
config_api  -s  ${CMADDR} -c config_topo

[ -f ${ELF} ]

# Generate default.elf
topo=`config_api -s ${CMADDR} -c config_topo`
HEIGHT=`echo $topo | awk  '{print $1}' |cut -f2 -d:`
WIDTH=`echo $topo | awk  '{print $2}' |cut -f2 -d:`
cself_default -x 0 -y 0 -w $WIDTH -h $HEIGHT --fabric_width $WIDTH --fabric_height $HEIGHT -o ${DEFAULT_ELF} --arch fyn

[ -f ${DEFAULT_ELF} ]
config_api -s ${CMADDR}  -c clear_state
config_api -c elf_start -s ${CMADDR}
config_api -c elf_write -s ${CMADDR} -e ${DEFAULT_ELF} --default_elf
config_api -c elf_write -s ${CMADDR} -e ${ELF}

env CEREBRAS_INI='profile_bins=false' test_amp_stream -c ${CMADDR} -f ${PWD}/../../wio_flows.json -s ${STREAMERS} -Z

config_api -c elf_done -s ${CMADDR}

echo "programming done" > pgm_done
