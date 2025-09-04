import base64
import logging

import kubernetes.config

CLUSTER_CONFIG_AUTH_SECRET_NAME = "cluster-auth"

logger = logging.getLogger("cs_cluster.k8s_api")


def _ensure_namespace(corev1: kubernetes.client.CoreV1Api, namespace: str):
    """Ensures that NAMESPACE has been created."""
    try:
        corev1.create_namespace(kubernetes.client.V1Namespace(metadata=kubernetes.client.V1ObjectMeta(name=namespace)))
        logger.debug(f"created namespace '{namespace}'")
    except kubernetes.client.exceptions.ApiException as e:
        if e.status != 409:
            raise


def put_cluster_authinfo(corev1: kubernetes.client.CoreV1Api, credential_file_contents: str,
                         namespace: str = "job-operator"):
    """
    Creates a secret in the k8s cluster storing authentication information.
    """
    _ensure_namespace(corev1, namespace)

    data = {
        ".ssh.conf": base64.b64encode(
            credential_file_contents.encode(),
        ).decode(),
    }
    secret = kubernetes.client.V1Secret(
        metadata=kubernetes.client.V1ObjectMeta(
            name=CLUSTER_CONFIG_AUTH_SECRET_NAME,
        ),
        data=data,
    )
    try:
        corev1.create_namespaced_secret(namespace, secret)
        logger.info(f"created new secret '{namespace}/{CLUSTER_CONFIG_AUTH_SECRET_NAME}'")
    except kubernetes.client.exceptions.ApiException as e:
        logger.debug(f"replace existing secret '{namespace}/{CLUSTER_CONFIG_AUTH_SECRET_NAME}'")
        if e.status == 409:
            corev1.delete_namespaced_secret(
                CLUSTER_CONFIG_AUTH_SECRET_NAME,
                namespace,
            )
            corev1.create_namespaced_secret(namespace, secret)
        else:
            raise
