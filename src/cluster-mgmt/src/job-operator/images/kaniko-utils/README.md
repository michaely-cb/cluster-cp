# Kaniko utils

## Environment variables

### Resolved during deploy
- `SHELL_XTRACE`
  - displays shell xtrace when set non-empty
  - example: `ENABLED`
- `CTR_DEBUG`
  - displays containerd verbose messages to help debug when set non-empty
  - example: `ENABLED`
- `BASE_IMAGE`
  - base image used to build a new image, can be overriden
  - example: `python:3.11`
- `PRIVATE_REGISTRY_SERVER_HOSTNAME`
  - service domain name of the private registry, will change in https://github.com/Cerebras/monolith/pull/71914
  - example: `registry.local`

### Resolved during runtime
- `DEPENDENCY_LIST`
  - passed in from requests
  - example: `["paramiko==2.10.3"]`
- `DESTINATION_IMAGE_TAG_SUFFIX`
  - calculated from package dependencies hash
  - example: d63335b7510c299c0a6b8cb58484eece

### Devived during runtime
- registry endpoint
  - source: hostpath-mounted `/etc/containerd/certs.d/${PRIVATE_REGISTRY_SERVER_HOSTNAME}/hosts.toml`
  - example: `x.x.x.x:5000`
- registry certificate
  - source: hostpath-mounted `/etc/containerd/certs.d/${PRIVATE_REGISTRY_SERVER_HOSTNAME}/hosts.toml`
  - example: `/opt/cerebras/certs/registry_tls.crt`
