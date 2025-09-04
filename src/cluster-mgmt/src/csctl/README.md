# csctl

A CLI for the Cerebras appliance.

## Building
```shell
# view all options
make help

# build
make build

# optionally copy the binary to your GOBIN
make install
```

## Docs

[csctl.md](csctl.md) contains usage information. This doc is shipped in the customer tarball so keep in mind that your
audience is the end-user when modifying this document. `csctl-reference.md` is generated with the proto-docs plugin for
protoc. Refer to its [template docs](https://github.com/pseudomuto/protoc-gen-doc/wiki/Custom-Templates) or
[template.go](https://github.com/pseudomuto/protoc-gen-doc/blob/master/template.go) file when 
modifying. Note that [sprig filters](http://masterminds.github.io/sprig) are available in the template context.