import logging

from deployment_manager.tests.conftest import MockCluster

logger = logging.getLogger(__name__)


def test_cluster_mgmt_cmds():
    with MockCluster("k8_update") as cluster:
        cluster.must_exec_root("/project/install.sh")
        cluster.must_exec_root(
            "cscfg profile create mocktest "
            "--inventory_file /host/config/inventory.csv --config_input /host/config/input.yml"
        )
        cluster.must_exec_root(f"cp /host/config/network_config.json /opt/cerebras/cluster-deployment/meta/mocktest/")

        # should fail if no pkg present
        rv, out, err = cluster.exec("rootserver", "cscfg cluster_mgmt update --no-confirm")
        assert rv != 0, f"expected failure, got: {rv}, {out} {err}"

        # tar.gz the mock csadm.sh and install at /opt/cerebras/packages
        pkg_dir = "cluster-pkg-0.0.0-000"
        cluster.must_exec_root(
            f"bash -c 'mkdir -p {pkg_dir} && cp -rf /host/mocks/* {pkg_dir} && tar czf {pkg_dir}.tar.gz {pkg_dir} && "
            f"mv {pkg_dir}.tar.gz /opt/cerebras/packages/'"
        )
        cluster.must_exec_root("cscfg cluster_mgmt update --no-confirm")
