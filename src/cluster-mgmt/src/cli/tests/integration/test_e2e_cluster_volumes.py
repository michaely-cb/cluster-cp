import logging
from typing import Callable

from click.testing import Result

from common.context import CliCtx
from common.models import SwitchConfig

logger = logging.getLogger("cs_cluster.test.e2e_cluster_volumes")


def test_cluster_volumes_put_validation(
    ctx: CliCtx, cs_cluster: Callable[[str], Result], use_custom_cluster_cm, use_cilium_ip_range,
):
    result = cs_cluster("deploy cluster-tools --force")
    assert result.exit_code == 0, result.stdout

    with use_custom_cluster_cm() as (cluster, update,):
        cluster.groups[0].switchConfig = SwitchConfig(
            subnet="10.1.0.0/24",
            gateway="10.1.0.254",
            parentnet="10.1.0.0/24",
            virtualStart="10.1.0.32",
            virtualEnd="10.1.0.253",
        )
        update(cluster)

        rv, so, se = ctx.cluster_ctr.exec(
            "cluster-volumes.sh put testvol0 --server 10.1.0.200 --server-path /test --container-path /test"
        )
        logger.info(f"testvol0 response: {so}\n{se}")
        assert rv != 0, so
        assert "10.1.0.200 falls within multus range" in se

    # add a fake cilium config map, then ensure we can't deploy volumes to the cilium range
    with use_cilium_ip_range("192.168.0.0/16"):
        rv, so, se = ctx.cluster_ctr.exec(
            "cluster-volumes.sh put testvol1 --server 192.168.0.12 --server-path /test --container-path /test"
        )
        logger.info(f"testvol1 response: {so}\n{se}")
        assert rv != 0, so
        assert "192.168.0.12 falls within cilium range" in se

    # try adding an invalid NFS server and ensure the add fails eventually
    rv, so, se = ctx.cluster_ctr.exec(
        "cluster-volumes.sh put testvol2 --server example.com --server-path /test --container-path /test"
    )
    logger.info(f"testvol2 response: {so}\n{se}")
    assert rv != 0, so
    assert "test failed to complete, there was probably a mount error" in se
