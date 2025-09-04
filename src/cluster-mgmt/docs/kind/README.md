# How to run e2e tests on dev kind cluster

## Installing the dependencies
e2e tests are known to work with:
- Go 1.21
- Protoc 3.15.8
- Kind 0.13
- Python 3.8

## Running the tests
- Customize the files `cs_pvc.yaml` and `cs_pv.yaml`. Deploy with `kubectl apply -f cs_pvc.yaml cs_pv.yaml`. This creates a required persistent volume.
- Deploy other components using the scripts in `src/cli/`. At a minimum:
    - Initialize K8s.
    - Deploy helm.
    - Deploy private-registry.
    - Deploy nginx.
    - Deploy job-operator.
    - Deploy cluster-server: `WSJOB_IMAGE='171496337684.dkr.ecr.us-west-2.amazonaws.com/cbcore:1.4.0-202207012350-48-78221f88' python3 cs_cluster.py deploy cluster-server.`
- Load the image you specified in `WSJOB_IMAGE` into your kind cluster.
- Customize the files in `e2e/ws/envs/kind`.
- Change directories to `e2e/ws/` and run `./stage-env.sh kind`.
- Run `cp -r ~/appliance-client/staged-run ./`.
- Run `make test`.
- You needn't go through this process each time you run e2es. For subsequent runs, run `cp -r ./staged-run ~/appliance-client/` and then `make test`.

## Accessing the logs
Logs appear in the `/opt/cerebras/` directory in the `kind-worker` container (07/12/2022.)
