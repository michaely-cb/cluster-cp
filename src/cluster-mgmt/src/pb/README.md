# Protobufs

Contains source files for protobufs. Attempts to match the structure of `monolith/src/pb`.

```sh
# compile the protos into Go files
make build

# clean up
make clean

# see all options
make help
```

## Go

Compiled protobufs are built into this directory. This module can then be used as a
common import for projects that require the protos.
