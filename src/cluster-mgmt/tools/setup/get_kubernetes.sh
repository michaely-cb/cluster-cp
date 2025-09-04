# --- Install Kubernetes (kubelet, kubectl, kubeadm)
cat <<EOF | sudo tee /etc/yum.repos.d/kubernetes.repo
[kubernetes]
name=Kubernetes
baseurl=https://packages.cloud.google.com/yum/repos/kubernetes-el7-\$basearch
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
exclude=kubelet kubeadm kubectl
EOF
sudo setenforce 0      # Set SELinux in permissive mode (effectively disabling it)
sudo sed -i 's/^SELINUX=enforcing$/SELINUX=permissive/' /etc/selinux/config
sudo yum install -y kubelet-1.24.4 kubectl-1.24.4 kubeadm-1.24.4 --disableexcludes=kubernetes
sudo systemctl enable --now kubelet
