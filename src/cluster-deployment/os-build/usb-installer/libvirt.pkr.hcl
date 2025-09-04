variable "libvirt_server" {
  type    = string
  default = "sc-r9ra14-s9.cerebrassc.local"
}

variable "sshkey" {
  type = string
}

variable "user" {
  type = string
}

variable "package_path" {
  type = string
}

variable "imagetag" {
  type    = string
  default = ""
}

locals {
  build_ts = formatdate("YYYYMMDD-HHmmss", timestamp())
  tag      = var.imagetag != "" ? var.imagetag : local.build_ts
}

source "libvirt" "usb-installer" {
  libvirt_uri            = "qemu+ssh://root@${var.libvirt_server}/system?no_verify=1&keyfile=${var.sshkey}"
  vcpu                   = 4
  memory                 = 4096
  network_address_source = "lease"

  network_interface {
    type    = "managed"
    network = "test"
  }

  volume {
    pool  = "builds"
    name  = "usb-installer-${local.tag}.qcow2"
    alias = "artifact"

    source {
      pool = "builds"
      # a 31G size volume - needs to be large enough to fit a Cluster.tar.gz (~21G)
      volume = "alpine3-vanilla-31g.qcow2"
      type   = "cloning"
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
  name    = "usb-installer"
  sources = ["source.libvirt.usb-installer"]

  provisioner "file" {
    source      = "entrypoint.sh"
    destination = "/"
  }

  provisioner "file" {
    source      = "root-profile"
    destination = "/root/.profile"
  }

  provisioner "shell" {
    scripts           = ["setup-fs.sh"]
    expect_disconnect = true
    skip_clean        = true
  }

  provisioner "shell" {
    pause_before = "5s"
    inline       = ["uptime"]
    max_retries  = 10
  }

  provisioner "shell" {
    scripts = ["setup-entrypoint.sh"]
  }

  provisioner "file" {
    source      = "${var.package_path}"
    destination = "/files/"
  }

  provisioner "shell" {
    inline = ["rm -f /files/empty"]
  }

  provisioner "shell" {
    inline = ["pkg=\"${basename(var.package_path)}\"; cd /files; if [ -f \"$${pkg}\" ]; then sha256sum \"$${pkg}\" > \"$${pkg}.sha256\"; fi"]
  }

  provisioner "file" {
    source      = "README_files"
    destination = "/files/README"
  }

  post-processor "manifest" {
    output = "manifest.json"
  }
}
