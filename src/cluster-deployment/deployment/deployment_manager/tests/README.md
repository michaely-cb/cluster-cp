# Integration tests

The integration tests run deployment manager within a docker container which is connected
to the same docker network as its deployment peers. Each peer runs `daemon.py` which serves
several purposes
- http server for mocking HTTP requests
- SSH server for allowing ansible/network_config to SSH to the container
- mocking shell calls on the machine

The latter is accomplished by mounting a folder containing mock responses into the container
and setting an env var to tell deamon.py where the mock responses are located. When daemon.py
starts, it will symlink itself to /bin/DIR for each DIR in the mocks parent folder so that it
can intercept shell commands.

For example, a mocks folder looks like
```
/mocks/
  foo/
    001_GetFoo.json
    002_PutFoo.json
  bar/
    001_DeleteBar.txt
```

where the deamon will symlink itself to /bin/foo and /bin/bar and once a shell session calls `foo`
`001_GetFoo.json` will be printed and all subsequent calls will print `002_PutFoo.json`.

When adding a new test, create a new folder under tests/assets/mocks/TEST_NAME and add your 
top-level commands there with the mock files under it. When create the MockCluster object, pass
in TEST_NAME to the constructor.

# Debugging

The MockCluster will stream logs from the containers. To get more details about test execution,
run pytest with log streaming:

```
pytest -olog_cli=true -olog_cli_level=INFO -v deployment_manager/tests
```

Additionally, if you need to inspect the state of the containers after the test, set envvar
`KEEP_CLUSTER=1` prior to running the test.