#!/bin/sh

if [ -n "${SHELL_XTRACE}" ]; then
  set -x
fi

console() {
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $@"
}

print_df() {
  console "Root partition usage:"
  df -h /
}

print_dockerfile() {
  fp="$1"

  echo ""
  if [ -f ${fp} ]; then
    echo "Dockerfile:"
    cat ${fp}
  else
    echo "ERROR: Expecting ${fp} to be a Dockerfile, but does not exist."
    exit 1
  fi
  echo ""
}
