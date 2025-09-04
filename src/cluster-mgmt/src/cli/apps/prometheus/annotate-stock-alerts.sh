#!/bin/bash

# This script reads a YAML file (owned by Cluster Mgmt) that maps alerts to
# dashboard URL annotations. For each prometheusrules, it adds the dashboard URL
# annotation to the corresponding alert from the mapping file.

if [ $# -eq 0 ]; then
    >&2 echo "Error: missing argument: dashboard annotations mapping YAML file"
    exit 1
fi
dashes_filename="$1"

# Validate the YAML in our annotations mapping file
yq --exit-status 'tag == "!!map" or tag== "!!seq"' "$dashes_filename" > /dev/null
exit_code=$?
if [ $exit_code -ne 0 ]; then
  >&2 echo "Error: invalid YAML file: $dashes_filename"
  exit $exit_code
fi

# For each prometheusrules that we want to patch
while IFS= read -r -d '' prometheusrules; do

  # Get the prometheusrules from k8s
  orig_filename=prometheusrules-${prometheusrules}.yaml
  updated_filename=prometheusrules-${prometheusrules}.updated.yaml
  kubectl -n prometheus get prometheusrules $prometheusrules -oyaml | yq > "$orig_filename"
  cp $orig_filename $updated_filename

  # For each dashboard annotation
  while IFS=$'\t' read -r alert_name dash_key dash_val; do

    # Warn if the alert wasn't found in the prometheusrules
    match_count=$(alert=$alert_name yq e '.. | select(has("alert") and .alert == strenv(alert)) | has("alert")' "$updated_filename" | wc -l)
    if [ $match_count -lt 1 ]; then
      echo "Warning: couldn't find alert $alert_name in $prometheusrules. Skipping this alert."
      continue
    fi

    # Annotate the corresponding alert in the prometheusrules YAML
    alert=$alert_name key=$dash_key val="$dash_val" \
      yq e '(.. | select(has("alert") and .alert == strenv(alert))).annotations.[env(key)] = strenv(val)' -i "$updated_filename"
  done < <(yq '.[] | select(.prometheusrules=="'"$prometheusrules"'") | .alerts.* | to_entries | .[] | (path | .[-2]) as $alert | [$alert, .key, .value] | join("'$'\t''")' <"$dashes_filename")

  # Apply (or "patch") the updated rules
  kubectl apply -f "$updated_filename"

done < <(yq '.[] | .prometheusrules' <"$dashes_filename" | while read -r line; do printf '%s\0' "$line"; done)