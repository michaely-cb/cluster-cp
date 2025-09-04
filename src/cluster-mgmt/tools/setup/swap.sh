echo "Added SWAP flags to kubelet config"
(echo "featureGates:" && echo "  NodeSwap: true" && echo "memorySwap:" && echo "  swapBehavior: LimitedSwap" && echo "failSwapOn: false") >> /var/lib/kubelet/config.yaml
systemctl restart kubelet
lsblk |grep nvme || swapon /dev/mapper/root_vg-swap
lsblk |grep nvme && swapon -a
free -h
systemctl restart kubelet
sudo sysctl --system
sudo systemctl enable --now containerd.service
systemctl restart containerd


# can be executely by:
# pssh -h all -I<./swap.sh