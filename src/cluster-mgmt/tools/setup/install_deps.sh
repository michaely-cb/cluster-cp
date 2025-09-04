sudo yum groupinstall "Development Tools" -y
sudo yum install epel-release -y
sudo yum install wget psmisc golang runc vim tex iproute-tc \
                 mosh cmake htop \
                 protobuf protobuf-c protobuf-c-devel \
                 protobuf-c-compiler protobuf-compiler \
                 python3-protobuf protobuf-lite -y
