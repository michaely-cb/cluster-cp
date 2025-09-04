packer {
  required_plugins {
    libvirt = {
      version = ">= 0.1"
      source = "github.com/thomasklein94/libvirt"
    }
    ansible = {
      version = ">= 1.0"
      source = "github.com/hashicorp/ansible"
    }
    git = {
      version = ">= 0.3.2"
      source = "github.com/ethanmdavidson/git"
    }
  }
}
