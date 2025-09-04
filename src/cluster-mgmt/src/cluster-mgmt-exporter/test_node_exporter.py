import json
import os
from node_exporter import NodeExporter


def test_describe_metrics() -> None:
    node_export = NodeExporter("test")
    metrics = {}
    for m in node_export.collect():
        metrics[m.name] = True
    assert "node_healthcheck_error" in metrics
    assert "node_nfs_server_health" in metrics
    assert "node_platform_version" in metrics
    assert "node_cluster_platform_version_mismatch" in metrics
    assert "node_bios_version_mismatch" in metrics
    assert "device_roce_status_mismatch" in metrics
    assert "node_bios_config_mismatch" in metrics


def test_node_health_check(tmp_path) -> None:
    with open(f"{tmp_path}/test_mount", "w") as fp:
        fp.write("10.245.8.9:/tmpfs /host/tmpfs \n")
        fp.write("100.67.244.117:6789:/volumes/compile/123 /host/var/lib/kubelet/pods/123/volumes/pv/mount ceph\n")

    os.environ.update({"PROC_MOUNT_PATH": f"{tmp_path}/test_mount",
                       "NODE_CHECK_CMD": "bash node-health-check.sh",
                       "ERROR_OUTPUT_FILE": f"{tmp_path}/errors.json",
                       "interfaces": "eth0",
                       "TEST_HANG_CMD": "invalid cmd",
                       "TEST_INTERFACE_CMD": "echo downgrade",
                       "TEST_CEPH_CONNECTION_CMD":
                           "echo 'libceph socket closed'| grep libceph | grep -E 'reset|closed|error'",
                       })
    node_export = NodeExporter("test")
    metrics = {}
    for m in node_export.collect():
        if m.name == "node_healthcheck_error":
            print(m.samples)
            for s in m.samples:
                if s.labels["error_type"] == "HostMountHang":
                    assert (s.labels["message"] ==
                            "host mount hang for fs_type ceph, mount_path /var/lib/kubelet/pods/123/volumes/pv/mount")
                    metrics["HostMountHang"] = True
                elif s.labels["error_type"] == "NicSpeedDowngraded":
                    assert s.labels["message"] == "speed downgraded for eth0 from lspci, nic needs replacement"
                    metrics["NicSpeedDowngraded"] = True
                elif s.labels["error_type"] == "CephConnectionError":
                    assert s.labels["message"] == "kernel logs showing ceph connection issues"
                    metrics["CephConnectionError"] = True
        metrics[m.name] = True
    assert "HostMountHang" in metrics
    assert "NicSpeedDowngraded" in metrics
    assert "CephConnectionError" in metrics


def test_node_version_check(tmp_path) -> None:
    versions = {
        "package": "v1",
        "config": "v2",
        "cbcore": ["v3", "v4"]
    }
    with open(f"{tmp_path}/version", "w") as fp:
        fp.write(json.dumps(versions))

    os.environ.update({"VERSION_FP": f"{tmp_path}/version", })
    node_export = NodeExporter("test")

    # test no marker will trigger label update
    v = ""
    for m in node_export.collect():
        if m.name == "node_platform_version":
            for s in m.samples:
                v = s.labels["cbcore"]
    assert v == "v3__v4"
    assert os.path.exists(f"{tmp_path}/.version.marker")
    last_modification_time = os.path.getmtime(f"{tmp_path}/.version.marker")

    # test no version change will not trigger label update
    v = ""
    for m in node_export.collect():
        if m.name == "node_platform_version":
            for s in m.samples:
                v = s.labels["cbcore"]
    assert v == "v3__v4"
    assert os.path.exists(f"{tmp_path}/.version.marker")
    current_modification_time = os.path.getmtime(f"{tmp_path}/.version.marker")
    assert last_modification_time == current_modification_time
    last_modification_time = current_modification_time

    # test version change will trigger label update
    versions["cbcore"] = ["v4"]
    with open(f"{tmp_path}/version", "w") as fp:
        fp.write(json.dumps(versions))
    v = ""
    for m in node_export.collect():
        if m.name == "node_platform_version":
            for s in m.samples:
                v = s.labels["cbcore"]
    assert v == "v4"
    assert os.path.exists(f"{tmp_path}/.version.marker")
    current_modification_time = os.path.getmtime(f"{tmp_path}/.version.marker")
    assert last_modification_time != current_modification_time
