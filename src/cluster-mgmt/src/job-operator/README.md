# job-operator

This directory contains the code relevant to cerebras job operators.

The cerebras job operator is built on top of Kubernetes APIs. It is inspired by
kubeflow [training operators](https://github.com/kubeflow/training-operator).

The local kubeflow was forked from https://github.com/kubeflow/common/tree/v0.4.1

We use [kubebuilder](https://book.kubebuilder.io/introduction.html) to generate the
skeleton of the code, and fill in our own logic.

The current WSJobController code is based on the `JobController` defined in
[`kubeflow/common` library](https://github.com/kubeflow/common).

This is the instruction of test/develop job-operator.

### Local develop with kind
If you are developing job-operator, the fastest way is to have a kind cluster ready,
and run job-operator locally without deploying image every time. 
```
make run
```
Note: Not recommended for new developers, you need to create kind cluster and install/update CRDs if necessary.
Check the following sections for more info.

### Deploy job-operator in local kind
If you want to test the entire workflow with build/deploy job-operator images, type the following:
It will auto create a kind cluster with name of "kind" if it's not existing yet.
```
make deploy
```

If you don't want to creat a new cluster, replace xxx with your own kind cluster name.
```
KIND_CLUSTER_NAME=xxx make deploy
```

After this, you should see job-operator pod running in `job-operator` namespace:

```
joykent@Joys-MacBook-Pro job-operator % kubectl get pod -n job-operator
NAMESPACE      NAME                                               READY   STATUS    RESTARTS   AGE
job-operator   job-operator-controller-manager-6c5c699d7b-4cd5q   2/2     Running   0          94s
job-operator   job-operator-controller-manager-6c5c699d7b-phtf9   2/2     Running   0          94s
```

The system is ready for submitting smoke WSJobs.
And you can view logs with:
```
kubectl logs -njob-operator job-operator-controller-manager-6c5c699d7b-4cd5q -f
```


### Run tests
Now you are all set to run tests.
#### unit/integration tests
```
make test
```

#### e2e tests
```
make e2e-test
```
