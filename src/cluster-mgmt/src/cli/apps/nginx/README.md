# Nginx Chart

See [github](https://github.com/kubernetes/ingress-nginx/tree/main/charts/ingress-nginx)

NOTE

During redeploy, nginx's previous generation controller pod lives for 12h before
terminating due to some combination of a long worker timeout and long termination
grace period.

If you want to kill the old generation of controllers, the way to do it is:

```bash
CRICTL="crictl -r unix:///run/containerd/containerd.sock"
CONTAINER_IDS=$(
      $CRICTL pods --namespace=ingress-nginx -o=json |
      jq -r '(.items[] | select(.labels["app.kubernetes.io/name"] == "ingress-nginx") | .id) // ""' |
      xargs -n1 -r $CRICTL ps -o=json --pod | jq -r '.containers[0].id // ""'
)
if [ -n "$CONTAINER_IDS" ]; then
  echo "$CONTAINER_IDS" | xargs -n1 $CRICTL stop
  echo "stopped nginx container(s)=$CONTAINER_IDS"
fi
```