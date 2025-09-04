packer {
  required_plugins {
    libvirt = {
      source  = "github.com/thomasklein94/libvirt"
      version = ">= 0.1"
    }
  }
}
