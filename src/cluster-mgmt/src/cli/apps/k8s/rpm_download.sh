#!/bin/bash
set -e

rpm_dir=${rpm_dir-/k8s-rpm}
download_dir=${rpm_dir}
# for permission in CI
if [ -n "${rpm_uid}" ] && [ -n "${rpm_gid}" ]; then
  download_dir=/tmp-k8s-rpm
fi
mkdir -p "${download_dir}"

# download rpm+ctr
function download_k8s_rpm() {
  version=$1
  # 1.30.4 => 1.30
  major_minor_version=$(echo "$version" | awk -F '.' '{print $1"."$2}')

  cat <<EOF >/etc/yum.repos.d/kubernetes.repo
[kubernetes]
name=Kubernetes
baseurl=https://pkgs.k8s.io/core:/stable:/v${major_minor_version}/rpm/
enabled=1
gpgcheck=1
gpgkey=https://pkgs.k8s.io/core:/stable:/v${major_minor_version}/rpm/repodata/repomd.xml.key
exclude=kubelet kubeadm kubectl
EOF

  mkdir -p "${download_dir}/${version}"
  yum clean metadata
  yumdownloader --disableexcludes=kubernetes --downloadonly --arch=x86_64 --destdir="${download_dir}/${version}" kubeadm-"${version}"
  yumdownloader --disableexcludes=kubernetes --downloadonly --arch=x86_64 --destdir="${download_dir}/${version}" kubectl-"${version}"
  yumdownloader --disableexcludes=kubernetes --downloadonly --arch=x86_64 --destdir="${download_dir}/${version}" kubelet-"${version}"
  yumdownloader --disableexcludes=kubernetes --downloadonly --arch=x86_64 --destdir="${download_dir}/${version}" cri-tools
  yumdownloader --disableexcludes=kubernetes --downloadonly --arch=x86_64 --destdir="${download_dir}/${version}" kubernetes-cni
}

# for centos7 docker image package purpose, repo has been archived
sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/CentOS-Base.repo
sed -i s/^#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/CentOS-Base.repo
sed -i s/^mirrorlist=http/#mirrorlist=http/g /etc/yum.repos.d/CentOS-Base.repo
if ! wget &>/dev/null; then
  yum install -y wget
fi

# skip ctr download if exists
ctr_version=${ctr_version-"1.7.20"}
wget -nc https://github.com/containerd/containerd/releases/download/v${ctr_version}/containerd-${ctr_version}-linux-amd64.tar.gz -P "${download_dir}"
# download rpm
versions=${k8s_versions-"1.24.4 1.25.16 1.26.15 1.27.16 1.28.13 1.29.9 1.30.4 1.31.10 1.32.6"}
for version in ${versions}; do
  download_k8s_rpm "$version"
done

if [ -n "${rpm_uid}" ] && [ -n "${rpm_gid}" ]; then
  groupadd -g "${rpm_gid}" group"${rpm_gid}" || true
  useradd -u "${rpm_uid}" -g "${rpm_gid}" -M -s /bin/false user"${rpm_uid}" || true
  chown "${rpm_uid}:${rpm_gid}" -R "${download_dir}"
  # mv can fail if already exists, for local testing case
  runuser -u user"${rpm_uid}" -- mv ${download_dir}/* "${rpm_dir}" || true
fi
