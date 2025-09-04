# Image related common functions

img_prune() {
  # clean up all stale images
  echo "cleaning up stale images on all nodes..."
  PSSH "$NERDCTL -n k8s.io image prune --all -f 2>&1>/dev/null" || true
}

# usage: img_pull IMAGE IMAGE_NAME [--load-mgmt]
# pulls IMAGE file as registry.local/IMAGE_NAME optionally on all mgmt nodes
img_pull() {
  image=$1
  image_name=$2
  load_mgmt=$3
  (
    mkdir -p ~/.docker
    configjson=$($KUBECTL get secret regcred --ignore-not-found -n "${NAMESPACE:-job-operator}" \
      -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d)
    if [ -n "$configjson" ]; then
      echo "$configjson" > ~/.docker/config.json
      echo "updated ~/.docker/config.json"
    fi
  )

  if [ -n "$load_mgmt" ] && has_multiple_mgmt_nodes; then
    PSSH_MGMT "mkdir -p ~/.docker"
    PSCP_MGMT "$HOME/.docker/config.json" "$HOME/.docker/config.json"
    if ! PSSH_MGMT "$NERDCTL -n k8s.io image pull --all-platforms $image" &>err; then
      echo "failed to pull image $image"
      cat err >&2
      return 1
    fi
  else
    $NERDCTL -n k8s.io image pull --all-platforms "$image"
  fi

  tagged_name=$registry_url/$image_name
  $NERDCTL -n k8s.io image tag "$image" "$tagged_name"
}

# usage: img_load IMAGE_PATH IMAGE_NAME [--load-mgmt]
# loads IMAGE_PATH file as registry.local/IMAGE_NAME optionally on all mgmt nodes
img_load() {
  local image_path image_name load_mgmt
  image_path=$1
  image_name=$2
  load_mgmt=$3

  # Don't load the image again if it already exists. The gzip decompress can take some time
  # especially for a large image.
  tagged_name="$registry_url/$image_name"
  id=$(nerdctl -n k8s.io image inspect --format '{{.Id}}' "$registry_url/$image_name" 2>/dev/null || echo "")
  if [[ -z "$id" ]]; then
    result=$(gzip --force --decompress -c "$image_path" | $NERDCTL -n k8s.io image load --all-platforms)
    if [[ ! "$result" =~ ^unpacking ]]; then
      echo "load image $image_path failed"
      return 1
    fi
    loaded_name=$(echo "$result" | head -n1 | awk '{print $2}')
    $NERDCTL -n k8s.io image tag "$loaded_name" "$tagged_name"
  else
    echo "Image $tagged_name already loaded to host, skipping reload"
  fi

  if [ -n "$load_mgmt" ] && has_multiple_mgmt_nodes; then
    image_file=$(basename "$image_path")
    PSCP_MGMT "$image_path" "/tmp/$image_file"
    load_image_cmd=(
      "id=\$(nerdctl -n k8s.io image inspect --format '{{.Id}}' \"$registry_url/$image_name\" 2>/dev/null || echo \"\");"
      "if [[ -z \"\$id\" ]]; then"
      "  result=\$(gzip --force --decompress -c /tmp/$image_file | $NERDCTL -n k8s.io image load --all-platforms);"
      "  if ! [[ \"\$result\" =~ ^unpacking ]]; then exit 1; fi;"
      "  loaded_name=\$(echo \"\$result\" | head -n1 | awk '{print \$2}');"
      "  $NERDCTL -n k8s.io image tag \$loaded_name \"$tagged_name\";"
      "fi"
    )
    PSSH_MGMT "${load_image_cmd[*]}"
  fi
}


img_push_registry() {
  local image retries
  image=$1
  retries=3
  for i in $(seq 1 $retries); do
    if $NERDCTL -n k8s.io image push --hosts-dir /etc/containerd/certs.d/ "$image" ; then
      break
    fi
    if [ "$i" -eq "$retries" ]; then
      echo "push image $image failed"
      return 1
    else
      echo "push image $image failed, retry $i"
      sleep 1
    fi
  done
}

# usage: img_load_all_nodes IMAGE [--nodes=comma,list,hostnames] [--tag=optional-retag]
img_load_all_nodes() {
  image=$1
  shift
  local nodes retag_image

  while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
    --nodes=*)
      # is this dead code?
      nodes="${key#*=}"
      shift
      ;;
    --tag=*)
      retag_image="${key#*=}"
      shift
      ;;
    *)
      shift
      ;;
    esac
  done

  # cleanup any stale jobs that missed script level cleanup that are older than 1 day
  one_day_ago=$(date -d "-1 day" +%s)
  $KUBECTL get ds -n ${SYSTEM_NAMESPACE} -lk8s-app=image-preload -ojson |
    jq -r '.items[] | select((.metadata.creationTimestamp | strptime("%Y-%m-%dT%H:%M:%SZ") | mktime) < '$one_day_ago') | .metadata.name' | while read -r ds; do
    echo "delete stale imageload ${ds}"
    $KUBECTL delete ds -n ${SYSTEM_NAMESPACE} "$ds" &>/dev/null || true
  done

  # Create a DS targetting the nodes specified by --nodes=x,y,...,z
  # note: this is mainly for cbcore loading although other images with the `sleep`
  # command on their PATH should work too
  imgsum=$(echo "${image}${nodes}" | md5sum | awk '{print $1}' | cut -c1-8)
  local dsname=image-load-${imgsum}-$(date +%s)
  cat <<EOF >"${dsname}.yaml"
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: ${dsname}
  namespace: ${SYSTEM_NAMESPACE}
  labels:
    k8s-app: image-preload
    instance: ${dsname}
spec:
  selector:
    matchLabels:
      name: ${dsname}
  template:
    metadata:
      labels:
        name: ${dsname}
        instance: ${dsname}
    spec:
      tolerations:
      - key: node-role.kubernetes.io/control-plane
        operator: Exists
        effect: NoSchedule
      - key: node-role.kubernetes.io/master
        operator: Exists
        effect: NoSchedule
      containers:
      - name: prepull
        image: ${image}
        command: ["sleep", "infinity"]
      terminationGracePeriodSeconds: 1
EOF
  if [ -n "${nodes}" ]; then
    cat <<EOF >>"${dsname}.yaml"
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
EOF
    IFS=','
    for node in $nodes; do
      cat <<EOF >>"${dsname}.yaml"
            - matchExpressions:
              - key: 'kubernetes.io/hostname'
                operator: In
                values:
                - ${node}
EOF
    done
    unset IFS
  elif [ -n "$INCREMENTAL_DIR" ]; then
    cat <<EOF >>"${dsname}.yaml"
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: cerebras/incremental-new
                operator: Exists
EOF
  fi

  $KUBECTL apply -f "${dsname}.yaml"
  trap "$KUBECTL delete -f ${dsname}.yaml &>/dev/null || true" RETURN EXIT
  sleep 1
  local timeout
  timeout="15m"
  echo "await preload image on every node for max $timeout"
  if ! $KUBECTL rollout status -n ${SYSTEM_NAMESPACE} "ds/${dsname}" --timeout="${timeout}"; then
    echo "failed to load image in time"
    return 1
  fi

  if [ -n "$retag_image" ]; then
    local nodelist=${NODE_LIST}
    if [ -n "$nodes" ]; then
      tr ',' '\n' <<<"$nodes" >nodes.list
      nodelist=nodes.list
    fi
    NODE_LIST=${nodelist} PSSH $NERDCTL -n k8s.io image tag "$image" "$retag_image"
  fi
}


# Check if image exists in registry
# usage: check_image_in_registry REPO_NAME TAG
check_image_in_registry() {
    local repo_name=$1
    local tag=$2
    local registry_host="127.0.0.1:5000"

    # Use manifest endpoint for more reliable checking (matches Python is_image_loaded)
    if curl -ks --fail --retry 3 --max-time 5 -o /dev/null \
        -H "Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json" \
        "https://${registry_host}/v2/${repo_name}/manifests/${tag}" 2>/dev/null; then
        echo "Image ${repo_name}:${tag} found in registry"
        return 0
    else
        echo "Image ${repo_name}:${tag} not found in registry"
        return 1
    fi
}

function clean_up_stale_registry() {
  if $NERDCTL -n k8s.io container inspect registry &>/dev/null; then
    $NERDCTL -n k8s.io container rm -f registry &>/dev/null
    echo "registry container removed" >&2
  else
    echo "registry container does not exists, skipping cleanup" >&2
  fi
}

# track common image version in pkg prop for other app's usage
function record_image_version() {
  image=$1
  version=$2
  yq -i ".properties.images.${image}=\"${version}\"" "${CLUSTER_PROPERTIES}"
}

function get_image_version() {
  image=$1
  if [ -f "${CLUSTER_PROPERTIES}" ]; then
    get_cluster_properties | jq -r ".properties.images.\"${image}\" // \"latest\""
  else
    $KUBECTL get cm job-operator-cluster-env -ojson | jq -r ".metadata.annotations.\"${image}-tag\" // \"latest\""
  fi
}

