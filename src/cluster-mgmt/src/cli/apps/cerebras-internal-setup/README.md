# Cerebras Internal Setup

## regcred-refresh

IT refreshes an ECR token every hour. This cronjob mounts the NFS or host filesystem and searches
for this file. If it is found and its contents are different, then the contents of the secret in
the cluster, it will replace the secret. This ensures that images can be pulled directly from ECR.
This service should therefore only be installed in Cerebras clusters and not in customer clusters.

Note: we build a special docker image with kubectl for this project containing kubectl. We were
using alpine/k8s public image prior to this but reverted to this lightweight approach (500mb ->
50mb).
