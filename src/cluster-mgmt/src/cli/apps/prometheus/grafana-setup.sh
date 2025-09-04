#!/bin/bash

set -e

MANAGED_FOLDERS="debug cluster-admin-unlocked job-admin ml-user hostio customer-dashboards resource-details ml-admin session-admin cluster-admin-locked VAST"
NETRC=/root/.netrc

user=$(</etc/secrets/grafana-credential/user)
pass=$(</etc/secrets/grafana-credential/password)
echo "machine prometheus-grafana.prometheus login $user password $pass" >$NETRC

# to handle spaces in group names in the for loops, only split on newline, not any space
orig_IFS=$IFS
IFS=$'\n'
# updating team/folder permissions
declare -A team_name_id_mapping
# create teams/users
for team in "cluster-admin" "ml-admin" "ml-user"; do
  echo "team $team"
  team_id=$(curl --netrc-file $NETRC -s http://prometheus-grafana.prometheus/api/teams/search?name=${team} | jq -r '.teams[0].id // empty ')
  if [ -z "${team_id}" ]; then
    echo "start creating team $team"
    team_id=$(curl --netrc-file $NETRC -s -X POST -H "Content-Type: application/json" -d '{"name": "'"${team}"'"}' http://prometheus-grafana.prometheus/api/teams | jq -r '.teamId // empty')
    if [ -z "${team_id}" ]; then
      echo "team:$team creation failed"
      exit 1
    fi
    echo "team $team created, id: $team_id"
  else
    echo "team $team already exists, id: $team_id"
  fi
  team_name_id_mapping["${team}"]="${team_id}"

  user_id=$(curl --netrc-file $NETRC -s http://prometheus-grafana.prometheus/api/users/lookup?loginOrEmail=${team} | jq -r '.id // empty ')
  if [ -z "${user_id}" ]; then
    echo "start creating user $team"
    user_id=$(curl --netrc-file $NETRC -s -X POST -H "Content-Type: application/json" -d '{"name": "'"${team}"'", "login": "'"${team}"'",  "password": "'"${team}"'"}' http://prometheus-grafana.prometheus/api/admin/users | jq -r '.id // empty')
    if [ -z "${team_id}" ]; then
      echo "user:$team creation failed"
      exit 1
    fi
    echo "user $team created, id: $user_id"
  else
    echo "user $team already exists, id: $user_id"
  fi
  curl --netrc-file $NETRC -s -X POST -H "Content-Type: application/json" -d '{"userId": '"${user_id}"'}' http://prometheus-grafana.prometheus/api/teams/"${team_id}"/members
  echo "user $team added to team $team"
done
for folder in $(curl --netrc-file $NETRC -s http://prometheus-grafana.prometheus/api/folders | jq -r .[] -c); do
  name=$(echo "$folder" | jq -r '.title')
  team_id=${team_name_id_mapping[$name]}
  if [ -n "${team_id}" ]; then
    echo "updating folder $name permission: $folder to team/admin only: $team_id"
    curl --netrc-file $NETRC -s -X POST -H "Content-Type: application/json" -d '{"items": [{"teamId": '"${team_id}"', "permission": 1}]}' http://prometheus-grafana.prometheus/api/folders/"$(echo "$folder" | jq -r '.uid')"/permissions
  #elif [ "$name" == "debug" ]; then
  elif [[ $MANAGED_FOLDERS =~ $name([[:space:]]|$) ]]; then
    echo "updating folder $name permission: $folder to editor/admin only"
    curl --netrc-file $NETRC -s -X POST -H "Content-Type: application/json" -d '{"items": [{"role": "Editor", "permission": 2}]}' http://prometheus-grafana.prometheus/api/folders/"$(echo "$folder" | jq -r '.uid')"/permissions
  fi
done
IFS=$orig_IFS # restore normal behavior

