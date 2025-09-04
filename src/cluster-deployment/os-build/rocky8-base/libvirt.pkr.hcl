data "git-repository" "cwd" {}
data "git-commit" "cwd-head" {}

variable "mirror_server" {
  type    = string
  default = "sc-r9ra14-s9.cerebrassc.local"
}

variable "libvirt_server" {
  type    = string
  default = "sc-r9ra14-s9.cerebrassc.local"
}

variable "sshkey" {
  type    = string
}

# Note that in create_patches mode, the created Rocky image is still fully
# functional with the latest patches and driver.  However, the .qcow2 image
# size will be bigger than the normal run (10GB vs 8GB) because of the extra
# disk activities in creating the Cerebras-patches repo and compiling the
# Mellanox driver.
variable "create_patches" {
  type    = bool
  default = false
}

variable "release" {
  type    = string
}

locals {
  git-head          = replace(data.git-repository.cwd.head, "/", "-")
  git-hash          = substr(data.git-commit.cwd-head.hash, 0, 7)
  git-dirty         = lookup({true="", false="-dirty"}, convert(data.git-repository.cwd.is_clean, string), "")
}

source "libvirt" "rocky8-base" {
  libvirt_uri            = "qemu+ssh://root@${var.libvirt_server}/system?no_verify=1&keyfile=${var.sshkey}"
  vcpu                   = 4
  memory                 = 4096
  network_address_source = "lease"

  network_interface {
    type    = "managed"
    network = "test"
  }

  volume {
    pool     = "builds"
    name     = "rocky8-base-${local.git-head}-${local.git-hash}${local.git-dirty}.qcow2"
    alias    = "artifact"
    capacity = "15G"

    source {
      type   = "cloning"
      volume = "rocky8-vanilla.qcow2"
      pool   = "builds"
    }
  }

  communicator {
    communicator                 = "ssh"
    ssh_username                 = "root"
    ssh_password                 = "root"
    ssh_bastion_host             = var.libvirt_server
    ssh_bastion_username         = "root"
    ssh_bastion_private_key_file = var.sshkey
  }
}

build {
  name    = "rocky8-base"
  sources = ["source.libvirt.rocky8-base"]

  provisioner "shell" {
    inline = [
      # resize volume if the disk has grown
      "parted /dev/sda resizepart 2 100%",
      "pvresize /dev/sda2",
      "lvextend /dev/rl/root -l +100%FREE ||:",
      "xfs_growfs /",
      # back up original repo files
      "cp /etc/yum.repos.d/Rocky-BaseOS.repo /etc/yum.repos.d/Rocky-AppStream.repo /tmp",
    ]
  }

  provisioner "file" {
    content     = templatefile("./Rocky-BaseOS.repo.pkrtpl.hcl", {mirror_server = var.mirror_server})
    destination = "/etc/yum.repos.d/Rocky-BaseOS.repo"
  }

  provisioner "file" {
    content     = templatefile("./Rocky-AppStream.repo.pkrtpl.hcl", {mirror_server = var.mirror_server})
    destination = "/etc/yum.repos.d/Rocky-AppStream.repo"
  }

  provisioner "shell" {
    inline = [
      "dnf install -y python38",
      "pip3 install -U pip==23.0",
      "pip install ansible==4.10.0",
    ]
  }

  provisioner "ansible-local" {
    playbook_file = "./playbook.yml"
    role_paths    = ["./roles/rocky8-base"]
    extra_arguments = ["--extra-vars", "'{create_patches: ${var.create_patches}, release: ${var.release}}'"]
  }

  provisioner "shell" {
    inline = [
      "systemctl reboot",
    ]
    expect_disconnect = "true"
    pause_after = "30s"
  }

  provisioner "ansible-local" {
    playbook_file = "./playbook2.yml"
    role_paths    = ["./roles/mellanox"]
    extra_arguments = ["--extra-vars", "'{create_patches: ${var.create_patches}, release: ${var.release}}'"]
  }

  provisioner "shell" {
    inline = [
      # restore original repo files
      "cp /tmp/Rocky-BaseOS.repo /tmp/Rocky-AppStream.repo /etc/yum.repos.d/",
      # cleanup
      "dnf clean all",
      "rm -f /root/anaconda-ks.cfg",
      "history -c",
    ]
  }

# Copy usernode related files to /opt/cerebras/usernode directory.
  provisioner "shell" {
    inline = [
      "mkdir -p /opt/cerebras/usernode"
    ]
  }

  provisioner "file" {
    source      = "../../init-scripts/init-usernode.sh"
    destination = "/opt/cerebras/usernode/"
  }

  provisioner "file" {
    source      = "../../os-provision/usernode/"
    destination = "/opt/cerebras/usernode/"
  }

  # In create_patches mode, download the /var/repo/Cerebras-patches.tar.gz
  # file from the virtual machine to
  # /var/www/html/repo/Rocky8/Cerebras-patches-${local.git-head}-${local.git-hash}${local.git-dirty}.tgz
  # on the mirror_server.
  # It's a bit messy because packer doesn't have conditional provisioner and 
  # I can't figure out a simple way to get the VM's IP from packer.
  provisioner "shell" {
    inline = [
      "mkdir -p /var/repo",
      "touch /var/repo/Cerebras-patches.tar.gz",
      "hostname -I > /root/vm_ip"
    ]
  }

  provisioner "file" {
    direction = "download"
    source    = "/root/vm_ip"
    destination = "./vm_ip"
  }

  provisioner "shell-local" {
    inline = [
      # Download the newly created security patch tarball to mirror_server
      # Note: xargs is used to trim the trailing whitespace
      "ssh root@${var.mirror_server} sshpass -p root scp -o StrictHostKeyChecking=no root@`cat vm_ip | xargs`:/var/repo/Cerebras-patches.tar.gz /var/www/html/repos/Rocky8/Cerebras-patches-${local.git-head}-${local.git-hash}${local.git-dirty}.tgz",
      # Remove the tarball in non-create-patches mode, when it will be empty
      "ssh root@${var.mirror_server} find /var/www/html/repos/Rocky8/Cerebras-patches-${local.git-head}-${local.git-hash}${local.git-dirty}.tgz -size 0 -delete",
      # Clean up"
      "rm vm_ip"
    ]
  }

  provisioner "shell" {
    inline = [
      # Clean up
      "rm -f /root/vm_ip /var/repo/Cerebras-patches.tar.gz"
    ]
  }

  post-processor "manifest" {
    output = "manifest.json"
  }
}

