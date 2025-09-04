#
# --- For resetting the Kubernetes install on any node (master or worker):
# https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/_print/#tear-down
#

kubeadm reset -f
iptables -F
iptables -t nat -F
iptables -t mangle -F
iptables -X
ip link delete cilium_vxlan ||:
ip link delete cilium_net ||:
rm -rf .kube/
rm -rf /etc/kubernetes/
rm -rf /var/lib/kubelet/
rm -rf /var/lib/etcd/
rm -rf ~/.kube/
rm -rf /usr/local/bin/cilium
rm -rf /usr/local/bin/helm
rm -rf /etc/cni/net.d/
swapoff -a
sudo systemctl restart containerd ||:


# can be executely by:
# pssh -h all -I<./reset.sh