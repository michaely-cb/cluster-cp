# --- MASTER: Initialize Kubernetes
#
# replace the advertise-address with the right NIC in config.yaml
sudo kubeadm init --config config.yaml

mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config

echo "----------"
echo "You should see the above content stored in 'kubeadm_init.out'"
echo "or kubeadm token create --print-join-command"
echo ""
echo "Make sure to note the above instructions for adding workers"
echo "to this cluster."

kubeadm token create --print-join-command

# for workers
# pssh -h all --inline "kubeadm join xxx"