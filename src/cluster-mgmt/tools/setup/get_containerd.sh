# --- Install CRI (containerd standalone)
#
#   Part 1   (answer 'A' and hit enter if reinstalling)
#
wget https://github.com/containerd/containerd/archive/v1.6.0.zip
unzip v1.6.0.zip
rm -f v1.6.0.zip
cd containerd-1.6.0
wget https://github.com/containerd/containerd/releases/download/v1.6.0/containerd-1.6.0-linux-amd64.tar.gz
tar xvf containerd-1.6.0-linux-amd64.tar.gz
sudo cp ./bin/* /usr/local/bin/.
sudo mkdir -p /etc/containerd
containerd config default > config.toml
sudo cp config.toml /etc/containerd/.
sudo cp containerd.service /etc/systemd/system/.
cd ..
rm -rf containerd-1.6.0
