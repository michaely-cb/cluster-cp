import json
import logging
import pytest
from pathlib import Path

from deployment_manager.tests.conftest import ASSETS_DIR, MockCluster
from deployment_manager.tools.switch.utils import interface_sort_key

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def link_cluster() -> MockCluster:
    cluster = MockCluster("link_cli")

    with cluster as cluster:
        cluster.must_exec_root("/project/install.sh")
        yield cluster


def test_link_cli(link_cluster: MockCluster):
    cluster = link_cluster
    cluster.must_exec_root(
        "cscfg profile create mocktest --config_input /host/config/input.yml"
    )
    for device in [
        "net001-sp-sw01 SW SP -p vendor.name=AR  management_credentials.user=admin  management_credentials.password=admin",
        "net001-lf-sw02 SW LF -p vendor.name=AR  management_credentials.user=admin  management_credentials.password=admin",
        
        "ar_mg_sw SW MG -p vendor.name=AR  management_credentials.user=admin  management_credentials.password=admin",
        "ju_mg_sw SW MG -p vendor.name=JU  management_credentials.user=admin  management_credentials.password=admin",
        "dl_mg_sw SW MG -p vendor.name=DL  management_credentials.user=admin  management_credentials.password=admin",
        
        "net002-lf-sw01 SW LF -p vendor.name=AR  management_credentials.user=admin  management_credentials.password=admin",
        "net001-mg-sr01 SR MG -p vendor.name=DL management_credentials.user=admin  management_credentials.password=admin",
        "net001-ax-sr01 SR AX -p vendor.name=DL management_credentials.user=admin  management_credentials.password=admin",
        "cs01 SY CS -p vendor.name=AR management_credentials.user=admin  management_credentials.password=admin",
        
        "ar_sr_1 SR MX -p vendor.name=DL management_credentials.user=admin  management_credentials.password=admin",
        "ar_sr_2 SR MX -p vendor.name=DL management_credentials.user=admin  management_credentials.password=admin",
        
        "ju_sr_1 SR MX -p vendor.name=DL management_credentials.user=admin  management_credentials.password=admin",
        "ju_sr_2 SR MX -p vendor.name=DL management_credentials.user=admin  management_credentials.password=admin",
        
        "dl_sr_1 SR MX -p vendor.name=DL management_credentials.user=admin  management_credentials.password=admin",
        "dl_sr_2 SR MX -p vendor.name=DL management_credentials.user=admin  management_credentials.password=admin",
    ]:
        cluster.must_exec_root(f"cscfg device add {device}")

    # add links from cmdline
    links = [
        "-s net001-sp-sw01:Eth1/1 -d net001-lf-sw02:Ethernet0 --speed 400",  # should be flipped automatically
        "-s net001-sp-sw01:Eth1/4 -d net002-lf-sw01:Ethernet4 --speed 400",
        "-s net001-mg-sr01:Eth1/1/1 -d net001-lf-sw02:Ethernet224 --speed 100",
        "-s net001-lf-sw02:Eth1/2 -d net001-ax-sr01:eth400g0 --speed 400",
        "-s net002-lf-sw01:Eth1/2 -d net001-ax-sr01:eth400g1 --speed 400",
        "-s net002-lf-sw01:Eth1/3 -d net001-mg-sr01:eth400g1 --speed 100",
 
        "-s ar_mg_sw:ma1 -d ar_sr_1:eno1np0 --speed 1",
        "-s ar_sr_2:eno1np0 -d ar_mg_sw:Eth2 --speed 1",     
        "-s ar_mg_sw:Eth3 -d ar_sr_1:@mgmt0 --speed 1",
        "-s ar_mg_sw:Eth4 -d ar_sr_1:@ipmi0 --speed 1",
        "-s ar_mg_sw:Eth5 -d ar_sr_1:@ipmi_mgmt0 --speed 1",
                  
        "-s ju_mg_sw:re0:mgmt-1 -d ju_sr_1:re0:eno1np1 --speed 1",
        "-s ju_sr_2:re0:eno1np1 -d ju_mg_sw:et-0/1/2 --speed 1",     
        "-s ju_mg_sw:et-0/1/3 -d ju_sr_1:re0:@mgmt0 --speed 1",
        "-s ju_mg_sw:et-0/1/4 -d ju_sr_1:re0:@ipmi0 --speed 1",
        "-s ju_mg_sw:et-0/1/5 -d ju_sr_1:re0:@ipmi_mgmt0 --speed 1",
                 
        "-s dl_mg_sw:Ma1 -d dl_sr_1:eno1np1 --speed 1",
        "-s dl_sr_2:eno1np1 -d dl_mg_sw:eth1/1/2 --speed 1",     
        "-s dl_mg_sw:eth1/1/3 -d dl_sr_1:@mgmt0 --speed 1",
        "-s dl_mg_sw:eth1/1/4 -d dl_sr_1:@ipmi_0 --speed 1",
        "-s dl_mg_sw:eth1/1/5 -d dl_sr_1:@ipmi_mgmt0 --speed 1",
    ]
    for link in links:
        cluster.must_exec_root(f"cscfg switch link add {link}")
    doc = json.loads(cluster.must_exec_root("cscfg switch link show -ojson"))
    assert len(doc) == len(links)

    assert len(json.loads(cluster.must_exec_root("cscfg switch link show -s net001-sp-sw01:Eth1/3 -ojson"))) == 0

    rv, out, _ = cluster.exec(
        "rootserver", "cscfg switch link add -s net001-sp-sw01:InvalidInterface -d net001-lf-sw02:Eth1/1 --speed 400"
    )
    assert rv != 0, f"expected failure due to invalid interface format, got: {rv} {out}"

    #Non-existent switch
    rv, out, _ = cluster.exec(
        "rootserver", "cscfg switch link add -s net000-lf-sw01:Eth1/1 -d net001-ax-sr01:eth400g1 --speed 400"
    )
    assert rv != 0, f"expected failure, got: {rv} {out}"

    # Missing source interface
    rv, out, _ = cluster.exec(
        "rootserver", "cscfg switch link add -s net002-lf-sw01 -d net001-ax-sr01:eth400g1 --speed 100"
    )
    assert rv != 0, f"expected failure, got: {rv} {out}"
    # add links from previous json output
    cluster.must_exec_root(f"bash -c 'cscfg switch link remove -y -ojson > /home/links.json'")
    doc = json.loads(cluster.must_exec_root("cscfg switch link show -ojson"))
    assert len(doc) == 0
    cluster.must_exec_root(f"cscfg switch link import /home/links.json")
    doc = json.loads(cluster.must_exec_root("cscfg switch link show -ojson"))
    assert len(doc) == len(links)

    # add links from csv format
    csv = "\n".join(
        [",".join([r["src_name"], r["src_if"], r["dst_name"], r["dst_if"], str(r["speed"])]) for r in doc])
    cluster.write_file_contents("rootserver", Path("/home/links.csv"),
                                "src_name,src_if,dst_name,dst_if,speed\n" + csv)
    cluster.must_exec_root("cscfg switch link remove -y")
    doc = json.loads(cluster.must_exec_root("cscfg switch link show -ojson"))
    assert len(doc) == 0
    cluster.must_exec_root(f"cscfg switch link import /home/links.csv")
    doc = json.loads(cluster.must_exec_root("cscfg switch link show -ojson"))
    assert len(doc) == len(links)

    # disallowed cases
    # LF - LF
    rv, out, _ = cluster.exec(
        "rootserver", "cscfg switch link add -s net002-lf-sw01:Eth1/50 -d net001-lf-sw02:Eth1/50 --speed 400"
    )
    assert rv != 0, f"expected failure, got: {rv} {out}"
    # NODE - NODE
    rv, out, _ = cluster.exec(
        "rootserver", "cscfg switch link add -s net001-ax-sr01:eth400g0 -d net001-ax-sr01:eth400g1 --speed 400"
    )
    assert rv != 0, f"expected failure, got: {rv} {out}"

    # null SRC interface
    rv, out, _ = cluster.exec(
        "rootserver", "cscfg switch link add -s net002-lf-sw01 -d net001-ax-sr01:eth400g1 --speed 100"
    )
    assert rv != 0, f"expected failure, got: {rv} {out}"

    # invalid speed
    rv, out, _ = cluster.exec(
        "rootserver", "cscfg switch link add -s net002-lf-sw01:Ethernet108 -d net001-ax-sr01:eth400g1 --speed 5"
    )
    assert rv != 0, f"expected failure, got: {rv} {out}"

    # non-existent switch
    rv, out, _ = cluster.exec(
        "rootserver", "cscfg switch link add -s net000-lf-sw01:Eth1/1 -d net001-ax-sr01:eth400g1 --speed 400"
    )
    assert rv != 0, f"expected failure, got: {rv} {out}"

    # add links from apstra fmt
    cluster.must_exec_root(f"cscfg switch link remove -y -ojson")
    rv, out, _ = cluster.exec("rootserver", f"cscfg switch link import --format apstra /host/files/apstra.json")
    assert rv != 0, "expected bad return code because not all links imported"
    doc = json.loads(cluster.must_exec_root("cscfg switch link show -ojson"))
    assert len(doc) == 2

    cluster.must_exec_root("cscfg profile delete mocktest")



def test_link_cli_sync(link_cluster: MockCluster):
    cluster = link_cluster
    cluster.must_exec_root(
        "cscfg profile create synctest --config_input /host/config/input.yml"
    )
    for device in [
        "net001-sp-sw01 SW SP -p vendor.name=AR management_credentials.user=admin  management_credentials.password=admin",
        "net001-lf-sw01 SW LF -p vendor.name=AR management_credentials.user=admin  management_credentials.password=admin",
        "net002-lf-sw01 SW LF -p vendor.name=AR management_credentials.user=admin  management_credentials.password=admin",
        "net001-mg-sr01 SR MG",
        "net001-mg-sr02 SR MG",
        "net001-ax-sr01 SR AX",
    ]:
        cluster.must_exec_root(f"cscfg device add {device}")

    # add links from cmdline
    links = [
        "-s net001-sp-sw01:Eth1/1 -d net001-lf-sw01:Eth1/1 --speed 400",
        "-s net001-sp-sw01:Eth1/5 -d net002-lf-sw01:Eth1/1 --speed 400",
        "-s net001-lf-sw01:Eth32/1 -d net001-mg-sr01:eth100g0 --speed 100",
        "-s net001-lf-sw01:Eth32/3 -d bo-0:port1 --speed 100",
        "-s net001-lf-sw01:Eth34/3 -d wk0:port1 --speed 100",
        "-s net002-lf-sw01:Eth32/1 -d ax:port1 --speed 400",
    ]
    for link in links:
        cluster.must_exec_root(f"cscfg switch link add {link}")
    doc = json.loads(cluster.must_exec_root("cscfg switch link show -ojson"))
    assert all(o["origin"] == "import" for o in doc)

    # network doc to update mgmt/ax node port names
    nw_doc = {
        "activation_nodes": [
            {"name": "net002-ax-sr01", "interfaces": [
                {"name": "eth400g0", "switch_name": "net002-lf-sw01", "switch_port": "Eth32/1"},  # should be updated
                {"name": "eth400g1", "switch_name": "net002-lf-sw01", "switch_port": "Eth33/1"},  # wasn't previously imported
            ]}
        ],
        "management_nodes": [
            {"name": "net001-mg-sr01", "interfaces": [
                {"name": "eth100g0", "switch_name": "net001-lf-sw01", "switch_port": "Eth32/1"},
            ]},
            {"name": "net001-mg-sr02", "interfaces": [
                {"name": "eth100g0", "switch_name": "net001-lf-sw01", "switch_port": "Eth32/3"},
            ]}
        ],
        "xconnect": {"connections": [
            {"links": [
                {"name": "net001-lf-sw01", "port": "Eth1/1"},  # existing, no change
                {"name": "net001-sp-sw01", "port": "Eth1/1"},
            ]},
            {"links": [
                {"name": "net001-sp-sw01", "port": "Eth2/5"},  # different source port, should update
                {"name": "net002-lf-sw01", "port": "Eth2/1"},
            ]}
        ]}
    }
    cluster.write_file_contents("rootserver", Path("/home/network_config.json"), json.dumps(nw_doc))
    cluster.must_exec_root(f"cscfg switch link sync -c /home/network_config.json -y")

    # prev spine link preserved
    out = cluster.must_exec_root("cscfg switch link show -ojson -f dst=net001-lf-sw01 src=net001-sp-sw01")
    assert len(json.loads(out)) == 1

    # other spine link updated
    out = cluster.must_exec_root("cscfg switch link show -ojson -f dst=net002-lf-sw01 src=net001-sp-sw01")
    links = json.loads(out)
    assert len(links) == 1
    assert links[0]["src_if"] == "Eth2/5"
    assert links[0]["dst_if"] == "Eth2/1"

    # spine link sanity
    out = cluster.must_exec_root("cscfg switch link show -ojson -f src=net001-sp-sw01")
    assert len(json.loads(out)) == 2

    # ax node updated
    out = cluster.must_exec_root("cscfg switch link show -ojson -f src=net002-lf-sw01")
    links = json.loads(out)
    assert len(links) == 2
    assert all(o["dst_name"] == "net002-ax-sr01" for o in links)

    # mg node updated
    out = cluster.must_exec_root("cscfg switch link show -ojson -f dst=net001-mg-sr01")
    links = json.loads(out)
    assert len(links) == 1
    assert links[0]["dst_if"] == "eth100g0"

    # worker retained
    out = cluster.must_exec_root("cscfg switch link show -ojson -f origin=import")
    links = json.loads(out)
    assert len(links) == 1
    assert links[0]["dst_name"] == "wk0"

    cluster.must_exec_root("cscfg profile delete synctest")


def test_link_repr_sort_key():
    for i, expected in {
            "et-0/0/12:5": "00000:00000:00012:00005", # juniper style
            "Eth365": "00365", # EC/DL style
            "Eth32/5": "00032:00005", # HPE/AR style
    }.items():
        actual = interface_sort_key(i)
        assert expected == actual
