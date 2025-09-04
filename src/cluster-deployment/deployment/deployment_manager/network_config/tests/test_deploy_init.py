import json
import pathlib
from unittest import mock

from deployment_manager.common.models import Cluster, MgmtNetworkConfig, MgmtVip
from deployment_manager.network_config.common.context import AppCtx, NetworkCfgCtx
from deployment_manager.network_config.common.task import ConfigTask
from deployment_manager.network_config.deploy import cmd_init
from deployment_manager.network_config.deployment import transformation_classes, ClusterSyncTask
from deployment_manager.network_config.deployment.base import DeploymentConfig

ASSETS_PATH = pathlib.Path(__file__).parent / "assets"


def test_init_from_master_config(tmp_path):
    mock_app_ctx = mock.Mock(spec=AppCtx)
    mock_app_ctx.get_clusters = mock.Mock(return_value=[
        Cluster(name="cluster1", mgmt_vip="1.1.1.1"),
        Cluster(name="cluster2", mgmt_vip="1.1.1.2"),
    ])
    mock_app_ctx.get_mgmt_network_config = mock.Mock(return_value=MgmtNetworkConfig(
        mgmt_vip=MgmtVip(vip="1.1.1.1", router_asn=65001, node_asn=65002)
    ))

    out_file = tmp_path / "init" / "network_config.json"
    out_file.parent.mkdir()
    master_config = str(ASSETS_PATH / "master_config_mb301.yaml")
    assert 0 == cmd_init(config_file=master_config, output_file=str(out_file), app_ctx=mock_app_ctx)[0]

    nw_cfg = json.loads(out_file.read_text())
    assert len(nw_cfg["clusters"]) == 2
    assert nw_cfg["clusters"][0]["mgmt_network"]["vip"] == "1.1.1.1"
    assert nw_cfg["clusters"][1]["mgmt_network"]["vip"] == "1.1.1.2"

    # run commands that transform master config to network json

    # update the mgmt vip on cluster 2
    mock_app_ctx.get_clusters = mock.Mock(return_value=[
        Cluster(name="cluster1", mgmt_vip="1.1.1.1"),
        Cluster(name="cluster2", mgmt_vip="1.1.1.0"),
    ])

    sys_conns_fp = str(ASSETS_PATH / "system_connections.json")
    cfg = DeploymentConfig(master_config, output_file=out_file)
    cfg.config["network"]["clusterNetwork"]["systemConnectionsFile"] = sys_conns_fp

    def _run_transformation(cls):
        if issubclass(cls, ConfigTask):
            with NetworkCfgCtx(
                cfg.network_config_filename,
                app_ctx=mock_app_ctx,
                persist_config_updates=True,
            ) as ctx:
                return cls(ctx).run()
        else:
            return cls(cfg).run()

    for cls in transformation_classes:
        _run_transformation(cls)

    net_doc = json.loads(out_file.read_text())
    assert len(net_doc["system_connections"]) == 1

    for s in net_doc['switches']:
        if s['tier'] == 'SP':
            assert 'router_id' in s

    assert net_doc["clusters"][0]["mgmt_network"]["vip"] == "1.1.1.1"
    assert net_doc["clusters"][1]["mgmt_network"]["vip"] == "1.1.1.0"

    # Ensure that removing a cluster is not allowed
    mock_app_ctx.get_clusters = mock.Mock(return_value=[
        Cluster(name="cluster1", mgmt_vip="1.1.1.1"),
    ])
    result = _run_transformation(ClusterSyncTask)
    assert not result.ok

