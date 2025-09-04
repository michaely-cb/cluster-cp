import json
import pathlib

from deployment_manager.tests.conftest import MockCluster

ASSETS_PATH = pathlib.Path(__file__).parent / "assets"


def test_cluster_cli():
    with MockCluster("cluster_cli") as cluster:
        cmds = [
            "/project/install.sh",
            "cscfg profile create devtest --config_input /host/config/devtest_input.yml",
            "cscfg device add mg-node0 SR MG",
            "cscfg device add mg-node1 SR MG",
            "cscfg device add mx-node0 SR MX",
            "cscfg device add mx-node1 SR MX",
        ]
        for cmd in cmds:
            cluster.must_exec_root(cmd)

        doc = json.loads(cluster.must_exec_root("cscfg cluster show -ojson"))
        assert len(doc) == 1

        doc = json.loads(cluster.must_exec_root("cscfg cluster device show -ojson"))
        assert len(doc) == 4

        cluster.must_exec_root("cscfg cluster device edit -y -f name=mg-node0 --controlplane=True")
        doc = json.loads(cluster.must_exec_root("cscfg cluster device show -ojson"))
        assert len([d for d in doc if d["controlplane"]]) == 1

        # add a cluster
        cluster.must_exec_root("cscfg cluster add devtest-green --dns-name=devtest -d green.devtest -m 123.45.67.89")
        res = cluster.must_exec_root("cscfg cluster show -ojson")
        assert len(json.loads(res)) == 2
        assert "devtest" in res
        assert "green.devtest" in res
        assert "123.45.67.89" in res

        # add a device to the cluster
        cluster.must_exec_root(
            "cscfg cluster device edit --cluster devtest-green -y -f name=mg-node1 --controlplane=True"
        )
        doc = json.loads(cluster.must_exec_root("cscfg cluster device show --cluster devtest-green -ojson"))
        assert len([d for d in doc if d["controlplane"]]) == 1
        doc = json.loads(cluster.must_exec_root("cscfg cluster device show -ojson"))
        assert len(doc) == 3

        # after removing, ensure the devices went back into the primary
        cluster.must_exec_root("cscfg cluster remove devtest-green -y")
        doc = json.loads(cluster.must_exec_root("cscfg cluster show -ojson"))
        assert len(doc) == 1
        doc = json.loads(cluster.must_exec_root("cscfg cluster device show -ojson"))
        assert len(doc) == 4
