import logging
import os
import pathlib
from typing import Tuple

import yaml

from deployment_manager.tests.conftest import MockCluster
from deployment_manager.tests_pb3.cluster import FileCopyCmd
from deployment_manager.tests_pb3.conftest import _sp_exec
from deployment_manager.tests_pb3.context import load_cli_ctx

logger = logging.getLogger(__name__)
CURRENT_DIR = pathlib.Path(__file__).parent

def test_migration_happy_path(kinder_cluster: Tuple[str, str]):
    """Test blue-green cluster migration with virtual address splitting.
    
    Virtual addresses are assigned based on cluster identity (position in the clusters array)
    rather than operation type. This means:
    - Blue cluster (index 0) always gets the first half of the virtual address range
    - Green cluster (index 1) always gets the second half of the virtual address range

    Note: The addresses don't change based on whether nodes are being added or removed.
    """
    blue_cluster, cp1_ip = kinder_cluster
    ctx = load_cli_ctx(None, blue_cluster)
    with MockCluster(
        "bg_migration", session_id=blue_cluster, has_network=False,
        extra_hosts={f"{blue_cluster}-control-plane-1": cp1_ip}
    ) as cluster:
        cluster.must_exec_root("/project/install.sh")
        config_data = {
            "basic": {
                "name": blue_cluster,
                "root_server": "rootserver"
            }
        }
        config_file = cluster.workdir / "host" / "config" / "input.yml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False)
        cluster.must_exec_root(
            "cscfg profile create bg-upgrade --config_input /host/config/input.yml"
        )
        _sp_exec(
            f"cp {CURRENT_DIR}/clusters/{blue_cluster}/network_config.json "
            f"{cluster.workdir}/host/config/network_config.json"
        )
        cluster.must_exec_root("mkdir -p /opt/cerebras/cluster-deployment/meta/bg-upgrade/")
        cluster.must_exec_root(f"cp /host/config/network_config.json /opt/cerebras/cluster-deployment/meta/bg-upgrade/")
        master_virtual_addr = cluster.must_exec_root(
            f"jq -r '.switches[0].virtual_addrs' /opt/cerebras/cluster-deployment/meta/bg-upgrade/network_config.json"
        )

        # add cluster devices
        cluster.must_exec_root(
            f"cscfg device add {blue_cluster}-control-plane-1 SR MG "
            f"-p kubernetes.controlplane=true -p management_credentials.password=root"
        )
        cluster.must_exec_root(f"cscfg device add {blue_cluster}-control-plane-2 SR MG -p kubernetes.controlplane=true")
        cluster.must_exec_root(f"cscfg device add {blue_cluster}-control-plane-3 SR MG -p kubernetes.controlplane=true")
        cluster.must_exec_root(f"cscfg device add {blue_cluster}-worker-1 SR WK")
        cluster.must_exec_root(f"cscfg device add switch0 SW LF")
        cluster.must_exec_root(f"cscfg deploy update --name=switch0 --stage=NETWORK_PUSH --status=COMPLETED -y")
        cluster.must_exec_root(
            f"cscfg deploy update --name={blue_cluster}-worker-1 "
            f"--stage=NETWORK_PUSH --status=COMPLETED -y"
        )
        cluster.must_exec_root(
            f"cscfg cluster device edit -f "
            f"name={blue_cluster}-control-plane-1 --controlplane=true -y"
        )

        # add green cluster
        cluster.must_exec_root(f"cscfg cluster add green-cluster -d green.example.com -m green.example.com")

        # create upgrade/batch
        cluster.must_exec_root(
            f"cscfg cluster upgrade create "
            f"--source {blue_cluster} --dest green-cluster --upgrade-pkg-path /opt/cerebras/packages/upgrade-pkg",
            environment=["SKIP_DEST_CLUSTER=true", "SKIP_DISK_SPACE_CHECK=true"]
        )
        cluster.must_exec_root(f"cscfg cluster upgrade batch create --force --device-list {blue_cluster}-worker-1")

        # copy the mock csctl
        ctx.cluster_ctr.exec_cmds([
            FileCopyCmd(
                CURRENT_DIR / "mock_csctl",
                "/usr/local/bin/mock_csctl"
            ),
            FileCopyCmd(
                CURRENT_DIR / "mock_csctl_test_migration_src",
                "/usr/local/bin/csctl"
            )
        ])
        ctx.cluster_ctr.exec(
            f"sed -i 's/CLUSTER_NAME_REPLACE/{blue_cluster}/g' /usr/local/bin/csctl"
        )

        # start migration
        cluster.must_exec_root(
            f"cscfg cluster upgrade batch migrate -y --skip-security-patch --force",
            environment=["SKIP_DEST_CLUSTER=true"]
        )
        # validation
        _, stdout, _ = ctx.cluster_ctr.exec(
            f"kubectl get nodes"
        )
        assert "control-plane-1" in stdout, stdout
        assert "worker-1" not in stdout, stdout
        assert "1.30" in stdout, stdout
        assert "1.31" not in stdout, stdout
        _, first_half_virtual_addr, _ = ctx.cluster_ctr.exec(
            f"jq -r '.switches[0].virtual_addrs' /opt/cerebras/cluster/network_config.json"
        )
        assert first_half_virtual_addr != master_virtual_addr
        _, name, _ = ctx.cluster_ctr.exec(
            f"jq -r '.name' /opt/cerebras/cluster/network_config.json"
        )
        assert name == blue_cluster
        
        _, modification_ts, _ = ctx.cluster_ctr.exec(
            f"stat -c %Y /opt/cerebras/cluster/network_config.json"
        )
        
        cluster.must_exec_root(
            f"cscfg cluster upgrade batch migrate --batch-id=1 -y --skip-security-patch --force",
            environment=["SKIP_DEST_CLUSTER=true"]
        )
        _, modification_ts_after, _ = ctx.cluster_ctr.exec(
            f"stat -c %Y /opt/cerebras/cluster/network_config.json"
        )
        assert modification_ts_after == modification_ts, "network_config.json should not be modified"

        # mock src upgrade after migrating
        stdout = cluster.must_exec_root(
            "cscfg cluster upgrade upgrade_src_cluster --skip-security-patch",
            environment=["K8S_UPGRADE_VERSION=1.31.10"]
        )
        stdout = cluster.must_exec_root("cscfg cluster upgrade show")
        # check if the status is SOURCE_CLUSTER_UPGRADED
        assert "SOURCE_CLUSTER_UPGRADED" in stdout, stdout
        _, stdout, _ = ctx.cluster_ctr.exec(
            f"kubectl get nodes"
        )
        assert "control-plane-1" in stdout, stdout
        assert "worker-1" not in stdout, stdout
        assert "1.31" in stdout, stdout
        assert "1.30" not in stdout, stdout

        # cancel the previous upgrade before creating a new one
        cluster.must_exec_root(
            "cscfg cluster upgrade cancel --upgrade-id 1 --force --skip-thanos-update"
        )
        # mock migration in by adding back to cluster
        cluster.must_exec_root(
            f"cscfg cluster upgrade create "
            f"--source green-cluster --dest {blue_cluster} --upgrade-pkg-path /opt/cerebras/packages/upgrade-pkg",
            environment=["SKIP_SRC_CLUSTER=true", "SKIP_DISK_SPACE_CHECK=true"]
        )
        # Find the manifest.json in the upgrade package
        _, stdout, _ = ctx.cluster_ctr.exec(
            f"find /opt/cerebras/packages/upgrade-pkg -name manifest.json"
        )
        assert "manifest.json" in stdout, "manifest.json not found in cluster package path"

        # backup manifest.json
        manifest_path = stdout.strip()
        cluster_pkg_path = os.path.dirname(manifest_path)
        ctx.cluster_ctr.exec(
            f"cp {manifest_path} /home/manifest.json.bak"
        )
        # rename manifest.json to manifest.retry.json
        # to check if it is picked during cluster upgrade
        ctx.cluster_ctr.exec(
            f"mv {manifest_path} {cluster_pkg_path}/manifest.retry123456.json"
        )
        # mock dest upgrade before migrating
        cluster.must_exec_root(
            "cscfg cluster upgrade upgrade_dest_cluster --upgrade-id 2 --skip-security-patch",
            environment=["K8S_UPGRADE_VERSION=1.31.10"]
        )
        # manifest.retry.json should be deleted after successful cluster upgrade
        _, stdout, _ = ctx.cluster_ctr.exec(
            f"ls {cluster_pkg_path}"
        )
        assert "manifest.retry" not in stdout, stdout
        
        # restore manifest.json for future tests
        ctx.cluster_ctr.exec(
            f"mv /home/manifest.json.bak {manifest_path}"
        )
        stdout = cluster.must_exec_root("cscfg cluster upgrade show --upgrade-id 2")
        assert "DEST_CLUSTER_UPGRADED" in stdout, stdout
        # start migration
        cluster.must_exec_root(
            f"cscfg cluster upgrade batch create "
            f"--upgrade-id=2 --force --device-list {blue_cluster}-worker-1",
            environment=["SKIP_SRC_CLUSTER=true"]
        )

        # copy the mock csctl
        ctx.cluster_ctr.exec_cmds([
            FileCopyCmd(
                CURRENT_DIR / "mock_csctl",
                "/usr/local/bin/mock_csctl"
            ),
            FileCopyCmd(
                CURRENT_DIR / "mock_csctl_test_migration_dst",
                "/usr/local/bin/csctl"
            )
        ])
        ctx.cluster_ctr.exec(
            f"sed -i 's/CLUSTER_NAME_REPLACE/{blue_cluster}/g' /usr/local/bin/csctl"
        )

        cluster.must_exec_root(
            f"cscfg cluster upgrade batch migrate --batch-id=2 -y --skip-security-patch --force",
            environment=["SKIP_SRC_CLUSTER=true"]
        )
        # validation
        _, stdout, _ = ctx.cluster_ctr.exec(
            f"kubectl get nodes"
        )
        assert "control-plane-1" in stdout, stdout
        assert "worker-1" in stdout, stdout
        assert "1.31" in stdout, stdout
        assert "1.30" not in stdout, stdout
        _, second_half_virtual_addr, _ = ctx.cluster_ctr.exec(
            f"jq -r '.switches[0].virtual_addrs' /opt/cerebras/cluster/network_config.json"
        )
        assert second_half_virtual_addr != master_virtual_addr
        assert first_half_virtual_addr == second_half_virtual_addr
        _, name, _ = ctx.cluster_ctr.exec(
            f"jq -r '.name' /opt/cerebras/cluster/network_config.json"
        )
        assert name == blue_cluster

        _, modification_ts, _ = ctx.cluster_ctr.exec(
            f"stat -c %Y /opt/cerebras/cluster/network_config.json"
        )
        
        cluster.must_exec_root(
            f"cscfg cluster upgrade batch migrate --batch-id=2 -y --skip-security-patch --force",
            environment=["SKIP_SRC_CLUSTER=true"]
        )
        _, modification_ts_after, _ = ctx.cluster_ctr.exec(
            f"stat -c %Y /opt/cerebras/cluster/network_config.json"
        )
        assert modification_ts_after == modification_ts, "network_config.json should not be modified"
