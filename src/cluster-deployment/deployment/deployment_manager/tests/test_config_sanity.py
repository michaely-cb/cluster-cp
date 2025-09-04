from deployment_manager.tests.conftest import MockCluster

def test_config_sanity():
    with MockCluster("config_sanity") as cluster:
        cluster.must_exec_root("/project/install.sh")
        cluster.must_exec_root("cscfg profile create mocktest --config_input /host/config/vendor_ingestion-input.yml")
        cluster.must_exec_root("cscfg device import_inventory /host/config/vendor_ingestion-inventory.csv -y")
        cluster.must_exec_root("cscfg device assign_mgmt_ips -y")
        # Edits with invalid format - should fail
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device edit wsx-wse001-mx-sr01 -p "
                                          "network_config.dhcp_boot_file=/var/ftpd/boot1.pxe management_info.mac=00:00:10:10:10")
        assert rc != 0, "Invalid MAC address format should have failed"
        assert "management_info.mac" in stderr and "invalid" in stderr.lower(), f"Expected MAC validation error in stderr: {stderr}"

        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device edit wsx-wse001-lf-sw01 -p "
                                          "management_info.ip=172.16.112.19a")
        assert rc != 0, "Invalid IP address format should have failed"
        assert "management_info.ip" in stderr and "invalid" in stderr.lower(), f"Expected IP validation error in stderr: {stderr}"
        
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device edit wsx-wse001-mg-sw01 -p "
                                          "switch_info.deploy_subnet=172.16.112.192/13")
        assert rc != 0, "Invalid subnet format should have failed"
        assert "switch_info.deploy_subnet" in stderr and "invalid" in stderr.lower(), f"Expected subnet validation error in stderr: {stderr}"
        
        # Edits duplicates - should fail
        # Set up initial values first
        cluster.must_exec_root("cscfg device edit wsx-wse001-lf-sw01 -p "
                               "management_info.ip=172.16.112.192")
        
        # Test duplicate IP
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device edit wsx-wse001-mg-sw01 -p "
                                          "management_info.ip=172.16.112.192")
        assert rc != 0, "Duplicate IP address should have failed"
        assert "Duplicate" in stderr and "management_info.ip" in stderr and "wsx-wse001-mg-sw01" in stderr and "wsx-wse001-lf-sw01", f"Expected duplicate IP error in stderr: {stderr}"

        # Gateway - management_info.ip should fail on other switches
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device edit wsx-wse001-lf-sw01 -p "
                                          "subnet_info.gateway=172.16.112.192")
        assert rc != 0, "Duplicate gateway should have failed"


        # Gateway - management_info.ip are allowed to be the same on the same mg switch
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device edit wsx-wse001-mg-sw01 -p "
                                          "management_info.ip=172.16.112.193")
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device edit wsx-wse001-mg-sw01 -p "
                                          "subnet_info.gateway=172.16.112.193")
        
        assert rc == 0, "Same switch gateway - mgmt_info.ip should succeed"
        

        # Set up initial MAC
        cluster.must_exec_root("cscfg device edit wsx-wse001-lf-sw01 -p "
                               "management_info.mac=00:00:10:10:10:10")
        
        # Test duplicate MAC
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device edit wsx-wse001-mg-sw01 -p "
                                          "management_info.mac=00:00:10:10:10:10")
        assert rc != 0, "Duplicate MAC address should have failed"
        assert "Duplicate" in stderr and "management_info.mac" in stderr.lower(), f"Expected duplicate MAC error in stderr: {stderr}"

        # Set up initial subnet
        cluster.must_exec_root("cscfg device edit wsx-wse001-lf-sw01 -p "
                               "subnet_info.subnet=172.16.112.192/26")
        
        # Test duplicate subnet
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device edit wsx-wse001-mg-sw01 -p "
                                          "subnet_info.subnet=172.16.112.192/26")
        assert rc != 0, "Duplicate subnet should have failed"
        assert "Duplicate" in stderr and "subnet_info.subnet" in stderr.lower(), f"Expected duplicate subnet error in stderr: {stderr}"

        # Set up initial management name
        cluster.must_exec_root("cscfg device edit wsx-wse001-lf-sw01 -p "
                               "management_info.name=xs12222")
        
        # Test duplicate management name
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device edit wsx-wse001-mg-sw01 -p "
                                          "management_info.name=xs12222")
        assert rc != 0, "Duplicate management info name should have failed"
        assert "Duplicate" in stderr and "name" in stderr.lower(), f"Expected duplicate name error in stderr: {stderr}"

        # config_sanity testing - should pass
        cluster.must_exec_root("cscfg device edit wsx-wse001-mg-sr01 -p "
                               "management_credentials.user=root management_credentials.password=xxx123")

        # Resolve issues in remaining devices
        cluster.must_exec_root("cscfg device edit -y -f location.stamp=stamp0 -p "
                                       "management_credentials.user=root management_credentials.password=xxx123")
        cluster.must_exec_root("cscfg device edit mc-001 -p "
                                "management_info.ip=111.22.3.4")
        cluster.must_exec_root("cscfg device edit wse002-cs-sy01 -p "
                                 "management_info.ip=111.22.3.6 "
                                 "management_info.name=xs10002")
        cluster.must_exec_root("cscfg device edit wsx-wse004-mc-sw01 -p "
                                    "management_info.ip=171.11.222.3")

        # Testing config_sanity filtering
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device config_sanity -f name!=xs10001")
        assert rc == 0, "config_sanity should pass after resolving issues"

        cluster.must_exec_root("cscfg device edit xs10001 -p "
                                "management_info.ip=170.82.115.2")
                               
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device config_sanity")
        assert rc == 0, "config_sanity should pass after resolving issues"

        # Test SY deployment required field
        cluster.must_exec_root("cscfg device edit wse002-cs-sy01 -p "
                                 "management_info.name=")
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device config_sanity")
        assert rc != 0, "config_sanity should fail due to empty management_info.name for SY"
        cluster.must_exec_root("cscfg device edit wse002-cs-sy01 -p "
                                 "management_info.name=xs10002")
        

        # Test SR deployment required field
        cluster.must_exec_root("cscfg device edit wsx-wse001-mx-sr01 -p "
                                 "ipmi_credentials.user= "
                                 "ipmi_credentials.password=")
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device config_sanity")
        assert rc != 0, "config_sanity should fail due to empty ipmi_credentials.user and ipmi_credentials.password for SR"
        cluster.must_exec_root("cscfg device edit wsx-wse001-mx-sr01 -p "
                                 "ipmi_credentials.user=xxx123 "
                                 "ipmi_credentials.password=xxx123")
        
        # Test general deployment required fields
        cluster.must_exec_root("cscfg device edit wsx-wse001-mg-sw01 -p "
                                 "management_credentials.user= "
                                    "management_credentials.password=")
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device config_sanity")
        assert rc != 0, "config_sanity should fail due to empty management_credentials.user and management_credentials.password for MG"
        cluster.must_exec_root("cscfg device edit wsx-wse001-mg-sw01 -p "
                                 "management_credentials.user=root "
                                 "management_credentials.password=xxx123")
        
        # Test alias and name uniqueness
        cluster.must_exec_root("cscfg device edit xs10001 -p "
                                 "management_info.name=xs10001 ")
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device config_sanity")
        assert rc != 0, "config_sanity should fail due to duplicate management_info.name"
        cluster.must_exec_root("cscfg device edit xs10001 -p "
                                 "management_info.name=")

        # Check if all issues are resolved
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device config_sanity")
        assert rc == 0, "config_sanity should pass after resolving all issues"

        # Forcing error to check if entry is blocked by validation errors
        cluster.must_exec_root("cscfg device add test-invalid-device SR WK")

        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device edit test-invalid-device --skip-validation -p "
                                          "management_info.ip=111.22.3.4")
        assert rc == 0, "Editing with --skip-validation should succeed even with existing validation errors"

        # Check if edit is allowed on mc-001 which has existing validation errors
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device edit mc-001 -p "
                                          "location.stamp=somestamp")
        assert rc != 0, "Edit should be blcoked on device with existing validation errors"

        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device config_sanity")
        assert rc != 0, "config_sanity should fail due to existing validation errors + deployment required fields missing"

        # Remove invalid device
        cluster.must_exec_root("cscfg device remove test-invalid-device")
        cluster.must_exec_root("cscfg device edit mc-001 --skip-validation -p "
                                    "management_info.ip=a.a.a.2")
        
        rc, stdout, stderr = cluster.exec("rootserver", "cscfg device config_sanity -f name!=mc-001")
        assert rc == 0, "config_sanity should pass when filtering out device with existing validation errors"
