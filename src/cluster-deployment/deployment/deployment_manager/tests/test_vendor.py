import ipaddress
import json
import pathlib
import textwrap

from deployment_manager.tests.conftest import MockCluster

ASSETS_PATH = pathlib.Path(__file__).parent / "assets"


def test_vendor_ingestion_flow():
    # test flow of vendor ingestion, (read input, assign ips, regenerate dnsmasq)

    with MockCluster("vendor_ingestion") as cluster:
        cluster.must_exec_root("/project/install.sh")
        cluster.must_exec_root("cscfg profile create mocktest --config_input /host/config/vendor_ingestion-input.yml")
        cluster.must_exec_root("cscfg device import_inventory /host/config/vendor_ingestion-inventory.csv -y")
        # below files are needed for switch ZTP test to work
        cluster.must_exec_root("mkdir -p /var/www/html")
        cluster.must_exec_root("touch /var/www/html/EOS64-4.33.1F.swi")
        cluster.must_exec_root("touch /var/www/html/EOS64-4.32.0F.swi")

        # some validation with show command
        def expect_devices(fstr: str, count: int):
            out = cluster.must_exec_root(f"cscfg device show -ojson --filter {fstr}")
            rv = json.loads(out)
            assert len(rv) == count, f"expected {count} devices from filter {fstr}, got {len(rv)}: {rv}"

        expect_devices("role=SP", 1)
        expect_devices("role=LF", 2)
        expect_devices("role=MC", 2)
        expect_devices("role=MA", 1)
        expect_devices("role=FE", 1)
        expect_devices("role=IX", 1)
        expect_devices("type=CN", 1)
        expect_devices("type=PR", 2)
        expect_devices("vendor.name=OG", 1)
        expect_devices("vendor.name=EN", 1)
        expect_devices("vendor.name=VE", 1)
        expect_devices("type=SW role=MG", 2)
        expect_devices("vendor.name=DL", 6)
        expect_devices("vendor.name=CR", 1)
        expect_devices("vendor.name!=DL", 15)
        expect_devices("name!~'.+'", 0)

        # assign IPs
        cluster.must_exec_root("cscfg device edit wsx-wse001-mx-sr01 -p "
                               "network_config.dhcp_boot_file=/var/ftpd/boot1.pxe management_info.mac=00:00:10:10:10:10")
        cluster.must_exec_root("cscfg device edit wsx-wse001-mg-sw01 -p "
                               "switch_info.deploy_subnet=172.16.112.192/26")
        cluster.must_exec_root("cscfg device assign_mgmt_ips -y")

        # assign system IP manually for test since expecting to get system dynamically through DHCP
        cluster.must_exec_root("cscfg device edit wse002-cs-sy01 -p "
                               "management_info.name=xs11111 management_info.ip=172.16.113.137")

        # ensure that a device in a rack without a mg switch does not get an IP
        # and that re-running after associating a switch with the rack assigns an IP
        out = cluster.must_exec_root(f"cscfg device show -ojson --filter name=mc-001")
        doc = json.loads(out)
        assert doc[0].get("properties", {}).get("management_info", {}).get("ip") is None

        cluster.must_exec_root(f"cscfg device edit -y -f name=wsx-wse002-mg-sw01 "
                               "-p switch_info.connected_racks=wse003,wse004")
        cluster.must_exec_root("cscfg device assign_mgmt_ips -y")
        out = cluster.must_exec_root(f"cscfg device show -ojson --filter name=mc-001")
        doc = json.loads(out)
        mc_ip = ipaddress.ip_address(doc[0].get("properties", {}).get("management_info", {}).get("ip"))
        assert mc_ip in ipaddress.ip_network("172.16.113.128/25"), "unexpected IP assignment for MC switch"

        def expect_prop_set(name: str, prop, attr, is_instance) -> str:
            out = cluster.must_exec_root(f"cscfg device show -ojson --filter name={name}")
            rv = json.loads(out)
            assert len(rv) == 1, f"device {name} not found: {rv}"
            try:
                v = rv[0]["properties"][prop][attr]
                is_instance(v)
                return v
            except KeyError:
                assert False, f"property {prop}.{attr} was not set for {name}"
            except:
                assert False, f"property {prop}.{attr} was not an instance of the expected type"

        expect_prop_set("wsx-wse001-mg-sw01", "subnet_info", "subnet", ipaddress.ip_network)
        expect_prop_set("wsx-wse001-mx-sr01", "management_info", "ip", ipaddress.ip_address)

        # generate switch ZTP files
        cluster.must_exec_root("mkdir -p /home/dnsmasq")
        rc, out, err = cluster.exec("rootserver", "cscfg switch generate_mgmt_config")
        assert rc != 0, f"expected err code since not all switches will be able to generate ZTP files, got: {rc}, {out}"
        out = cluster.must_exec_root("bash -c 'ls /var/ftpd/*.ztp'").strip().split("\n")
        assert len(out) == 3, f"expected ztp files not generated, generated: {out}"
        out = cluster.must_exec_root("bash -c 'ls /var/ftpd/*.startup_config'").strip().split("\n")
        assert len(out) == 3, f"expected startup_config files not generated, generated: {out}"

        # test media converter config generation
        cluster.must_exec_root("cscfg device edit mc-001 -p switch_info.mac_verified=True")
        cluster.must_exec_root("cscfg switch mediaconverter generate_mgmt_config")
        mc_cfg = cluster.must_exec_root("cat /var/ftpd/mc-001.startup_config")
        assert "${" not in mc_cfg, "untemplated variable found in mediaconverter startup config"

        # regenerate dnsmasq using the ip-allocator dnsmasq config format
        # simulate the creation of ZTP configs
        cluster.must_exec_root("cscfg device edit wsx-wse002-mx-sr01 -p "
                               "network_config.dhcp_option_67=boot1.pxe")
        cluster.must_exec_root("cscfg device edit wsx-wse001-mg-sw01 -p "
                               "switch_info.mac_verified=True subnet_info.lease_time=1h")

        cluster.must_exec_root("mkdir -p /home/dnsmasq")
        cluster.must_exec_root("cscfg dhcp update --dryrun", environment={"OUTPUT_DIR": "/home/dnsmasq"})
        cluster.must_exec_root("dnsmasq --test -C /home/dnsmasq/wsx-wse001-mg-sr01/dnsmasq.conf")
        dnsmasq_conf = cluster.must_exec_root("cat /home/dnsmasq/wsx-wse001-mg-sr01/dnsmasq.conf")
        with open("/tmp/x.conf", 'w') as f:
            f.write(dnsmasq_conf)
        target_dnsmasq_conf = (ASSETS_PATH / "config" / "vendor_ingestion-dnsmasq.conf").read_text()
        assert dnsmasq_conf.strip().endswith(target_dnsmasq_conf.strip()), \
            f"dnsmasq config generation differed from expected\n########## Generated ##########\n{dnsmasq_conf}\n########## Expected ##########\n{target_dnsmasq_conf}"
        server_lines = [line for line in dnsmasq_conf.splitlines() if line.startswith("server=")]
        assert server_lines == ["server=1.1.1.1"], "expected a single upstream server"
        rc, _, _ = cluster.exec("rootserver", "grep dynamic /home/dnsmasq/wsx-wse001-mx-sr01/dnsmasq.conf")
        assert rc == 1, "expected dynamic ranges to be cleared from mirror configs"

        # fake a lease file with a lease for system xs10001 and see that system MAC is updated in dnsmasq conf
        lease_doc = textwrap.dedent(f"""
        1714843562 3c:ec:ef:7a:a5:86 172.28.8.243 sc-r1rb1-s9 01:3c:ec:ef:7a:a5:86
        1714843562 70:55:f8:01:01:01 172.16.113.230 xs10001 *
        """)
        cluster.must_exec_root(f"mkdir -p /var/lib/dnsmasq/")
        cluster.write_file_contents("rootserver", pathlib.Path("/var/lib/dnsmasq/dnsmasq.leases"), lease_doc)
        cluster.must_exec_root("cscfg system assign_mgmt_ips -y")
        cluster.must_exec_root("cscfg dhcp update --dryrun", environment={"OUTPUT_DIR": "/home/dnsmasq"})
        dnsmasq_conf = cluster.must_exec_root("cat /home/dnsmasq/wsx-wse001-mg-sr01/dnsmasq.conf")
        assert "\ndhcp-host=set:type_sy,70:55:f8:01:01:01,172.16.113.230,xs10001\n" in dnsmasq_conf

        # clear IP assignments and then ensure using smaller prefixes works
        cluster.must_exec_root("cscfg device edit -y -f name=~. -p subnet_info.subnet= management_info.ip= ipmi_info.ip= subnet_info.gateway=")
        cluster.must_exec_root("cscfg device edit -y -f name=wsx-wse001-mg-sw01 -p subnet_info.prefixlen=26")
        cluster.must_exec_root("cscfg device assign_mgmt_ips -y")
        sn = expect_prop_set("wsx-wse001-mg-sw01", "subnet_info", "subnet", ipaddress.ip_network)
        sn = ipaddress.ip_network(sn)
        assert sn.prefixlen == 26

        # clear this switch's subnet and ensure the DHCP generation doesn't crash
        cluster.must_exec_root("cscfg device edit -y -f name=wsx-wse001-mg-sw01 -p subnet_info.subnet=''")
        cluster.must_exec_root("cscfg dhcp update --dryrun", environment={"OUTPUT_DIR": "/home/dnsmasq"})
