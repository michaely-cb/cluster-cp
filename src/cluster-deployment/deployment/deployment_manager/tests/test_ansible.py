import logging

import pytest

from deployment_manager.tests.conftest import MockCluster

logger = logging.getLogger(__name__)


@pytest.mark.skip("Skip as it depends on a real server availability")
def test_ansible_task():
    cluster = MockCluster("validator")

    with cluster:
        cluster.must_exec_root("/project/install.sh")
        
        cluster.must_exec_root("cscfg create_profile mocktest --config_input /host/config/ansible-input.yml")
        cluster.must_exec_root("cscfg device import_inventory /host/config/ansible-inventory.csv -y")

        cluster.must_exec_root("cscfg server health check")
        cluster.must_exec_root(
            "cscfg update_status --name sc-r1rb1-s4 --stage NETWORK_PUSH --status COMPLETED"
        )
        
        output = cluster.must_exec_root(
            "cscfg deploy --stage install_os --section precheck"
        )
        print(output)
        
        cluster.must_exec_root(
            "cscfg update_status --name sc-r1rb1-s5 --stage OS_INSTALLATION --status COMPLETED"
        )
        
        # expect ansible to fail due to missing mlxup binary
        output = cluster.must_exec_root(
            "cscfg deploy --stage deploy_nodes"
        )
        print(output)

        cluster.must_exec_root(
            "cscfg update_status --name sc-r1rb1-s5 --stage PROVISIONING --status COMPLETED"
        )
        
        # expect ansible to fail due to missing mlxup binary
        output = cluster.must_exec_root(
            "cscfg deploy --stage configure_nics"
        )
        print(output)
