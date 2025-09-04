import unittest

from pathlib import Path

from deployment_manager.tools.kubernetes.interface import KubernetesCtl
from deployment_manager.tools.ssh import ExecMixin, SSHConn

ASSETS_DIR = Path(__file__).parent / "assets"

def test_get_cluster_device_names():
    kctl = KubernetesCtl("test_profile", "test_dn", "user", "pass")
    mock_ssh = unittest.mock.Mock(spec=ExecMixin)

    # empty cluster yaml
    mock_ssh.exec.return_value = (0, "", "")
    kctl._conn = mock_ssh
    cluster_devs = kctl.get_cluster_device_names()
    assert not cluster_devs, f"Expected no cluster devices, got {cluster_devs}"

    # mock cluster yaml
    mock_ssh.exec.return_value = (0, (ASSETS_DIR / "files" / "k8s_cluster.yaml").read_text(), "")
    cluster_devs = kctl.get_cluster_device_names()
    assert len(cluster_devs) == 17, f"Expected 17 cluster devices, got {len(cluster_devs)}"

    # error case
    mock_ssh.exec.return_value = (-1, "", "")
    cluster_devs = kctl.get_cluster_device_names()
    assert not cluster_devs

def test_create_aws_ecr_credential_secret():
    kctl = KubernetesCtl("test_profile", "test_dn", "user", "pass")
    mock_ssh = unittest.mock.Mock(spec=SSHConn)

    kctl._conn = mock_ssh
    credential_name = "aws-ecr-credential"
    account_id = "1234567890"
    region = "us-east-1"
    temp1="temp1fileforkey"
    temp2="temp2fileforsecret"
    mock_ssh.exec.side_effect = [
        (0, temp1, ""), # mktemp
        (0, temp2, ""), # mktemp
        (0, "", ""), # kubectl
        (0, "", ""), # rm
        (0, "", ""), # rm
    ]

    ret = kctl.create_aws_ecr_credential_secret(credential_name, "key", "secret", account_id, region)
    assert ret == 0

    assert mock_ssh.scp_file.call_count == 2
    assert mock_ssh.scp_file.call_args_list[0].args == ("key", temp1)
    assert mock_ssh.scp_file.call_args_list[1].args == ("secret", temp2)

    assert mock_ssh.exec.call_count == 5
    kubectl_call_string = mock_ssh.exec.call_args_list[2].args[0]
    assert f"create secret generic {credential_name}" in kubectl_call_string
    assert f"--from-literal=account_id={account_id}" in kubectl_call_string
    assert f"--from-literal=region={region}" in kubectl_call_string
    assert f"--from-file=key={temp1}" in kubectl_call_string
    assert f"--from-file=secret={temp2}" in kubectl_call_string
