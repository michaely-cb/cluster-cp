#!/usr/bin/env bash
CFG_DIR="/opt/cerebras"
OUT="/var/lib/node_exporter/textfile_collector/session_membership.prom"

mkdir -p "$(dirname "$OUT")"

# clean file to keep metrics unique
: > "$OUT"

echo "# HELP cerebras_session_member 1 if a session exists in this namespace" >> "$OUT"
echo "# TYPE cerebras_session_member gauge" >> "$OUT"
namespaces=$(cat "$CFG_DIR/config_v2" | jq -r '.clusters[0].namespaces[] | .name')
for ns in $namespaces; do
    echo "cerebras_session_member{namespace=\"${ns}\"} 1"
done >> "$OUT"
