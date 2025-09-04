#!/bin/bash

# Script to chmod world writable to all the subdirs under the given parent directory, and stop
# at the subdirectories with a given pattern.
#
# For example,
#     ./chmod_writable_to_subdirs /cb/tests/compile_root cs_*
#
# will chmod world writable on subdirectories under /cb/tests/compile_root, and stop at `cs_xxxx`
# subdirectories.

set -e

# Chmod subdirs to be world-writable.
function chmod_writable_to_subdirs() {
  local root_dir=$1
  local curr_dir=$2

  while [ "$curr_dir" != "$root_dir" ]; do
    curr_dir=${curr_dir%/*}
    chmod a+w "$curr_dir"
  done
}

function main() {
    local parent_dir=$1
    local subdir_pattern=$2

    if [[ $(stat -c %A "$parent_dir") == *?w? ]]; then
    echo "$parent_dir is already world writable"
    return 0
  fi

  find "$parent_dir" -name "$subdir_pattern" -exec bash -c 'dirname {}' \; | uniq > /tmp/chmod_subdirs.txt
  if [ -s /tmp/chmod_subdirs.txt ]; then
    while IFS= read -r subdir; do
      if [[ $subdir == $parent_dir* ]]; then
        chmod a+w "$subdir"
        chmod_writable_to_subdirs "$parent_dir" "$subdir"
      fi
    done < /tmp/chmod_subdirs.txt
  fi

  chmod a+w "$parent_dir"
}

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <parent_dir> <subdir_pattern>"
    exit 1
fi

parent_dir=$1
subdir_pattern=$2
main "$parent_dir" "$subdir_pattern"
