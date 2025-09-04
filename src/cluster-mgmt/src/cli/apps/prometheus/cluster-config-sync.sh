#!/bin/bash

set -e

if [ -z "$KUBECTL" ]; then
  KUBECTL="kubectl"
fi
if [ -z "$SYSTEM_NAMESPACE" ]; then
  SYSTEM_NAMESPACE="job-operator"
fi

function config_sync() {
  cm=$1
  source=$2
  target=$3
  tmp_file="$cm.yaml"

  # Get the ConfigMap from the source namespace
  # for testing purpose create if not exist
  if ! $KUBECTL get cm "$cm" -n"$source" 2>/dev/null; then
    $KUBECTL create configmap "$cm" -n"$source"
  fi
  $KUBECTL get cm "$cm" -n "$source" -o yaml >"$tmp_file"

  # Remove metadata fields that should not be copied
  sed -i '/namespace:/d' "$tmp_file"
  sed -i '/resourceVersion:/d' "$tmp_file"
  sed -i '/creationTimestamp:/d' "$tmp_file"
  sed -i '/uid:/d' "$tmp_file"
  sed -i '/selfLink:/d' "$tmp_file"

  if ! $KUBECTL -n "$target" get cm "$cm" 2>/dev/null; then
    $KUBECTL create -f "$tmp_file" -n "$target"
  else
    $KUBECTL replace -f "$tmp_file" -n "$target"
  fi
}

config_sync cluster "$SYSTEM_NAMESPACE" prometheus
config_sync cluster-server-volumes "$SYSTEM_NAMESPACE" prometheus
