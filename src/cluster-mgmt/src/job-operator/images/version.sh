#!/usr/bin/env bash

# use shasum to generate image version
# usage: generate_image_version content_path image_name filter0 filter1 ...
function generate_image_version() {
  path=$1
  image=$2

  if [ -f "$path" ]; then
    folder=$(dirname "$path")
    shasum "$path" | cut -c 1-10 >"$folder/_version_$image"
  else
    filter=()
    for ((i = 3; i <= $#; i++)); do
      pattern="${!i}"
      if [[ "$pattern" == *"/"* ]]; then
        flag="-path"
        pattern="$path/$pattern"
      else
        flag="-name"
      fi
      filter+=("$flag" "$pattern" "-o")
    done
    # default to all if no filter provided
    if [ ${#filter[@]} -eq 0 ]; then
      filter+=("-name" "*" "-o")
    fi
    unset 'filter[${#filter[@]}-1]'
    folder="$path"
    # LC_ALL=C sort will get consistent order for both Mac/linux
    # https://stackoverflow.com/a/28903/1695885
    find "${path}" -type f '(' "${filter[@]}" ')' -print0 | LC_ALL=C sort -zf | xargs -0 shasum | shasum | cut -c 1-10 >"$path/_version_$image"
  fi
  cat "$folder/_version_$image"
}

generate_image_version "$@"
