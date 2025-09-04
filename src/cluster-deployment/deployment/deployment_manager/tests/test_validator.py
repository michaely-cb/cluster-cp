import logging

import pytest

from deployment_manager.tests.conftest import MockCluster

logger = logging.getLogger(__name__)


@pytest.mark.skip("Skip as it depends on devcluster availability")
def test_validator():
    cluster = MockCluster("validator", has_network=False)
    
    with cluster:
        cluster.must_exec_root("/project/install.sh")
        cluster.must_exec_root(
            "cscfg profile create mocktest "
            "--inventory_file /host/config/test-validator.csv --config_input /host/config/input.yml"
        )
        cluster.must_exec_root("cscfg change_profile mocktest")
        cluster.must_exec_root("cscfg update_status --name rootserver --stage NETWORK_PUSH --status COMPLETED")
        cluster.must_exec_root("cscfg server health force-update --degraded --attr ipmi_access --status OK")
        cluster.must_exec_root("cscfg server health force-update --degraded --attr server_health --status OK")
        cluster.must_exec_root("cscfg server health force-update --degraded --attr mgmt_link --status OK")
        cluster.must_exec_root("cscfg server health force-update --degraded --attr ipmi_lease --status OK")
        cluster.must_exec_root("cscfg deploy --stage deploy_root_server --section precheck")
        cluster.must_exec_root("cscfg deploy --stage deploy_root_server --section validate")
        cluster.must_exec_root("cscfg update_status --stage OS_INSTALLATION --status STARTED")
        cluster.must_exec_root("cscfg update_status --name rootserver --stage NETWORK_PUSH --status COMPLETED")
        cluster.must_exec_root("cscfg deploy --stage install_os --section validate")
        print(cluster.must_exec_root("cscfg show deployment"))
        cluster.must_exec_root("cscfg update_status --stage PROVISIONING --status STARTED")
        cluster.must_exec_root("cscfg update_status --name rootserver --stage NETWORK_PUSH --status COMPLETED")
        cluster.must_exec_root("cscfg deploy --stage deploy_nodes --section validate")
        print(cluster.must_exec_root("cscfg show deployment"))
        cluster.must_exec_root("cscfg update_status --stage NIC_COLLECTION --status STARTED")
        cluster.must_exec_root("cscfg update_status --name rootserver --stage NETWORK_PUSH --status COMPLETED")
        cluster.must_exec_root("cscfg deploy --stage configure_nics --section validate")
        print(cluster.must_exec_root("cscfg show deployment"))
        
        
         
