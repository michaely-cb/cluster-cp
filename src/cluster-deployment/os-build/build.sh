#!/bin/bash

# Wrapper for packer builds to enforce reproducibility

set -eo pipefail

# flags
SKIP_COMMIT=false
CREATE_PATCHES=false
DEBUG=false
TARGET=
RELEASE=
while [ ! $# -eq 0 ]; do
  case "$1" in
    # skip the git commit
    --skip-commit | -s)
      SKIP_COMMIT=true
      ;;
    # use release specific security patches
    --release | -r)
      shift
      RELEASE=$1
      ;;
    # create the latest greatest security patches in OS build
    --create-patches | -c)
      CREATE_PATCHES=true
      ;;
    # step through the build process
    --debug | -d)
      DEBUG=true
      ;;
    */*)
      TARGET=${1//\//}
      ;;
    -*)
      echo "ERROR: $1 is not a valid flag"
      exit 1
      ;;
  esac
  shift
done

function fail {
  echo "you have uncommitted changes; please commit them first"
  exit 1
}

# if committing, don't allow builds unless git state is clean
if [[ "$SKIP_COMMIT" = false ]] ; then
  git diff-index --quiet HEAD -- || fail
fi

# target dir file
if [[ ! -d $TARGET ]]; then
  echo "target $TARGET is not found"
  exit 1
fi

# release must be specified, unless the --create-patches option is set
if [[ "$RELEASE" = "" && "$CREATE_PATCHES" = false ]]; then
  echo "Release (e.g. 2.4.0) is not specified"
  exit 1
fi

if [[ "$DEBUG" = true ]] ; then
  export PACKER_FLAGS=-debug
fi

git_hash=$(git rev-parse --short HEAD)

# setup
mkdir -p out
logfile=$(pwd)/out/$TARGET-$git_hash.out

cd $TARGET

# run packer
export PKR_VAR_sshkey=${PKR_VAR_sshkey:-$HOME/.ssh/id_rsa}
export PKR_VAR_user=$USER
export PKR_VAR_create_patches=$CREATE_PATCHES
export PKR_VAR_release=$RELEASE

packer init .
packer build -timestamp-ui $PACKER_FLAGS . |& tee -a $logfile

# commit manifest
if [[ "$SKIP_COMMIT" = false ]] ; then
  git add manifest.json
  artifact_id=$(cat manifest.json | jq -r '.builds[-1].artifact_id')
  git commit -m "created new image for $TARGET from commit $git_hash" -m "artifact $artifact_id"
else
  echo "skipping git commit; output log is in $logfile"
fi
