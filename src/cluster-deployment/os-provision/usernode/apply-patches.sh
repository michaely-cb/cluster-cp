#!/bin/bash
#
# Apply security patches to current usernode

USAGE="Usage: apply_patches.sh --deploy_server <deploy_server> [--no_prompt]"

# Extract deploy server
DEPLOY_SERVER=
NO_PROMPT=0

while [ ! $# -eq 0 ]; do
  case "$1" in
    -d|--deploy_server)
      DEPLOY_SERVER=$2
      shift 2
      ;;

    -h|--help)
      echo $USAGE
      exit 0
      ;;

    --no_prompt)
      NO_PROMPT=1
      shift 1
      ;;

    -*)
      echo "ERROR: $1 is not a valid flag"
      echo $USAGE
      exit 1
      ;;

    *)
      echo "ERROR: $1 is not a valid argument"
      echo $USAGE
      exit 1
      ;;
  esac
done

if [[ -z "$DEPLOY_SERVER" ]]; then
  echo "ERROR: <deploy_server> is not defined"
  echo $USAGE
  exit 1
fi

if ! ping -W2 -c 2 $DEPLOY_SERVER >& /dev/null; then
  echo "ERROR: cannot ping deploy server $DEPLOY_SERVER"
  exit 1
fi

# Launch the real script
cd /opt/cerebras/usernode
if ! ansible-playbook apply-patches.yml --extra-vars "{deploy_server: $DEPLOY_SERVER}"; then
  exit 1
fi

if ! needs-restarting -r; then
  if [ $NO_PROMPT == 0 ]; then
    read -p "Reboot requested due to security patches. Press <enter> to reboot."
  else
    echo "Rebooting due to security patches..."
  fi
  /sbin/reboot
fi

echo "Successfully applied the patches to the usernode"
