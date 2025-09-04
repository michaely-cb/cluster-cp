# WSJob Examples

This directory contains some examples of WSJob.

## Deploy job-operator and build test image

To test locally, you need to build/deploy the job-operator first.
Go to cluster-mgmt/src/job-operator/Readme for details.

## Create `wsjob-simple`

The yaml file for `wsjob-simple` is `simple.yaml`. We will create `wsjob-simple` WSJob with this command:

```
kubectl apply -f simple.yaml
```

After this, you should see the relevant pods running in `job-operator` namespace:

```
kubectl get pods -njob-operator -o wide
NAMESPACE            NAME                                               READY   STATUS      RESTARTS       AGE    IP            NODE                 NOMINATED NODE   READINESS GATES
job-operator         job-operator-controller-manager-5c4dc648bf-5wvfr   2/2     Running     0              34m    10.244.0.45   kind-control-plane   <none>           <none>
job-operator         job-operator-controller-manager-5c4dc648bf-6xzqc   2/2     Running     0              34m    10.244.0.44   kind-control-plane   <none>           <none>
job-operator         wsjob-simple-activation-0                          0/1     Completed   0              19s    10.244.0.60   kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-0                     0/1     Init:0/1    0              20s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-1                     0/1     Init:0/1    0              20s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-10                    0/1     Init:0/1    0              20s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-11                    0/1     Init:0/1    0              20s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-2                     0/1     Init:0/1    0              20s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-3                     0/1     Init:0/1    0              20s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-4                     0/1     Init:0/1    0              20s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-5                     0/1     Init:0/1    0              20s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-6                     0/1     Init:0/1    0              20s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-7                     0/1     Init:0/1    0              20s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-8                     0/1     Init:0/1    0              20s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-9                     0/1     Init:0/1    0              20s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-command-0                             0/1     Completed   0              20s    10.244.0.58   kind-control-plane   <none>           <none>
job-operator         wsjob-simple-coordinator-0                         0/1     Completed   0              20s    10.244.0.57   kind-control-plane   <none>           <none>
job-operator         wsjob-simple-weight-0                              0/1     Completed   0              20s    10.244.0.61   kind-control-plane   <none>           <none>
job-operator         wsjob-simple-weight-1                              0/1     Completed   0              20s    10.244.0.59   kind-control-plane   <none>           <none>```
```
When WSJob completes, all relevant pods will be in `Completed` state:

```
NAMESPACE            NAME                                               READY   STATUS      RESTARTS       AGE     IP            NODE                 NOMINATED NODE   READINESS GATES
job-operator         job-operator-controller-manager-5c4dc648bf-5wvfr   2/2     Running     0              36m     10.244.0.45   kind-control-plane   <none>           <none>
job-operator         job-operator-controller-manager-5c4dc648bf-6xzqc   2/2     Running     0              36m     10.244.0.44   kind-control-plane   <none>           <none>
job-operator         wsjob-simple-activation-0                          0/1     Completed   0              2m13s   10.244.0.60   kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-0                     0/1     Completed   0              2m14s   172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-1                     0/1     Completed   0              2m14s   172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-10                    0/1     Completed   0              2m14s   172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-11                    0/1     Completed   0              2m14s   172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-2                     0/1     Completed   0              2m14s   172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-3                     0/1     Completed   0              2m14s   172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-4                     0/1     Completed   0              2m14s   172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-5                     0/1     Completed   0              2m14s   172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-6                     0/1     Completed   0              2m14s   172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-7                     0/1     Completed   0              2m14s   172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-8                     0/1     Completed   0              2m14s   172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-broadcastreduce-9                     0/1     Completed   0              2m14s   172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-simple-command-0                             0/1     Completed   0              2m14s   10.244.0.58   kind-control-plane   <none>           <none>
job-operator         wsjob-simple-coordinator-0                         0/1     Completed   0              2m14s   10.244.0.57   kind-control-plane   <none>           <none>
job-operator         wsjob-simple-weight-0                              0/1     Completed   0              2m14s   10.244.0.61   kind-control-plane   <none>           <none>
job-operator         wsjob-simple-weight-1                              0/1     Completed   0              2m14s   10.244.0.59   kind-control-plane   <none>           <none>```
```

The `wsjob-simple` shows `Succeeded` state:

```
joykent@Joys-MacBook-Pro job-operator % kubectl get wsjob -n job-operator
NAME           STATE       AGE
wsjob-simple   Succeeded    5m
```

## Create `wsjob-fail`


This yaml file for `wsjob-fail` is `fail.yaml`. We will create `wsjob-fail` WSJob with this command:

```
kubectl apply -f fail.yaml
```

After this, you should see the relevant pods running in `job-operator` namespace:

```
joykent@Joys-MacBook-Pro job-operator % kubectl get pod -n job-operator -o wide -w
NAME                                               READY   STATUS              RESTARTS   AGE
NAMESPACE            NAME                                               READY   STATUS      RESTARTS       AGE    IP            NODE                 NOMINATED NODE   READINESS GATES
job-operator         job-operator-controller-manager-5c4dc648bf-5wvfr   2/2     Running     0              40m    10.244.0.45   kind-control-plane   <none>           <none>
job-operator         job-operator-controller-manager-5c4dc648bf-6xzqc   2/2     Running     0              40m    10.244.0.44   kind-control-plane   <none>           <none>
job-operator         wsjob-fail-activation-0                            0/1     Completed   0              39s    10.244.0.67   kind-control-plane   <none>           <none>
job-operator         wsjob-fail-broadcastreduce-0                       0/1     Init:0/1    0              39s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-fail-broadcastreduce-1                       0/1     Init:0/1    0              39s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-fail-broadcastreduce-10                      0/1     Init:0/1    0              39s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-fail-broadcastreduce-11                      0/1     Init:0/1    0              39s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-fail-broadcastreduce-2                       0/1     Init:0/1    0              39s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-fail-broadcastreduce-3                       0/1     Init:0/1    0              39s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-fail-broadcastreduce-4                       0/1     Init:0/1    0              39s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-fail-broadcastreduce-5                       0/1     Init:0/1    0              39s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-fail-broadcastreduce-6                       0/1     Init:0/1    0              39s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-fail-broadcastreduce-7                       0/1     Init:0/1    0              39s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-fail-broadcastreduce-8                       0/1     Init:0/1    0              39s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-fail-broadcastreduce-9                       0/1     Init:0/1    0              39s    172.18.0.2    kind-control-plane   <none>           <none>
job-operator         wsjob-fail-command-0                               0/1     Error       0              39s    10.244.0.69   kind-control-plane   <none>           <none>
job-operator         wsjob-fail-weight-0                                0/1     Completed   0              39s    10.244.0.68   kind-control-plane   <none>           <none>
job-operator         wsjob-fail-weight-1                                0/1     Completed   0              39s    10.244.0.66   kind-control-plane   <none>           <none>```
```

wsjob-fail-command-0` will fail very quickly, and enter `Error` state.
The `wsjob-fail` WSJob shows `Failed` state:

```
joykent@Joys-MacBook-Pro job-operator % kubectl get wsjob -n job-operator
NAME           STATE       AGE
wsjob-fail     Failed      10m
```
