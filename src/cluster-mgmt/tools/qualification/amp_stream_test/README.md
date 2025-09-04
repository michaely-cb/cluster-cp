# amp_stream_test

This test runs `test_amp_stream` in k8s. It is used to validate the BR ingress/egress
performance on specified ports.

Internally, this test constructs a WSJob spec, which creates 3 different roles:

* `Chief`

This role is responsible for programming the fabric with a given loopback kernel.
There will be 1 for each wafer.

* `Weight`

This role calls to `test_amp_stream` to test the BR ingress/egress performance.

* `BroadcastReduce`

This role starts the BR processes.

## Build the package
Type this command to create a tarball to package this test:
```
make build
```

## Run the test

This test should be run on the management node. Run this command to initiate the test:
```
./run_amp_stream_test.sh --help
Usage: ./run_amp_stream_test.sh [--help] [--num-wafers] [--drop] [--streamers] \
  [--schedules] [--sample-size] [--egress-amplify] [--eth-port]
```