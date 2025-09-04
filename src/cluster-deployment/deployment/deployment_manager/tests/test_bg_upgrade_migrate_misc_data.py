import pytest
import yaml
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)))
import deployment_manager.cli.migrate_config_and_secret as mcs

class DummyCtl:
    """Mocks KubernetesCtl: records commands and returns predefined outputs."""
    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []
    def run(self, cmd):
        self.calls.append(cmd)
        for key, val in self.responses.items():
            if key in cmd:
                return val
        return (0, "", "")  # Default: success, no output

    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): return False

@pytest.fixture(autouse=True)
def patch_kubernetes_ctl(monkeypatch):
    # For code that constructs KubernetesCtl
    monkeypatch.setattr(mcs, 'KubernetesCtl', lambda *a, **kw: DummyCtl())
    yield

def test_strip_k8s_metadata_removes_known_fields():
    resource = {
        "metadata": {
            "name": "foo",
            "resourceVersion": "rv",
            "uid": "uid",
            "creationTimestamp": "ts",
            "managedFields": "mf",
            "selfLink": "link",
            "keep": "stay"
        }
    }
    out = mcs.strip_k8s_metadata(resource)
    meta = out["metadata"]
    for key in ["resourceVersion", "uid", "creationTimestamp", "managedFields", "selfLink"]:
        assert key not in meta
    assert meta["keep"] == "stay"

@pytest.mark.parametrize("labels,should_label", [
    ({}, True),
    ({"bg_migration": "foo"}, False)
])
def test_add_label_labels_when_needed(labels, should_label):
    ctl = DummyCtl({
        "kubectl get": (0, yaml.dump({"metadata": {"labels": labels}}), ""),
        "kubectl label": (0, "", "")
    })
    # Should not raise even if resource does not exist or get fails
    mcs.add_label(ctl, "configmap", "cm1", "ns", "bg_migration", "foo")
    called = any("kubectl label" in c for c in ctl.calls)
    assert called == should_label

def test_add_label_skips_on_get_failure():
    ctl = DummyCtl({"kubectl get": (1, "", "err")})
    # Should not raise, but skip labeling
    mcs.add_label(ctl, "configmap", "foo", "ns", "bg_migration", "foo")
    # Only get call should be made, no label calls
    assert any("kubectl get" in c for c in ctl.calls)
    assert not any("kubectl label" in c for c in ctl.calls)

def test_fetch_resource_yaml_success_and_skips_on_failure():
    good = DummyCtl({"kubectl get": (0, yaml.dump({"foo": 123}), "")})
    assert mcs.fetch_resource_yaml(good, "configmap", "foo", "ns") == {"foo": 123}
    bad = DummyCtl({"kubectl get": (2, "", "fail")})
    # Should return None or skip, not raise
    result = mcs.fetch_resource_yaml(bad, "configmap", "foo", "ns")
    assert result is None

def test_backup_resource_adds_both_labels(monkeypatch):
    cmds = {
        # First check if resource exists (returns the resource, so it exists)
        "kubectl get configmap foo -n ns --ignore-not-found": (0, "NAME   DATA   AGE\nfoo    1      1d", ""),
        # Then the backup commands
        "kubectl get configmap foo -n ns -o json |": (0, "", ""),
        "jq '.metadata.name = \"foo-backup\"'": (0, "", ""),
        "kubectl replace --force -n ns -f -": (0, "", ""),
        "kubectl label configmap foo-backup -n ns bg_migration-": (0, "", ""),
        "kubectl label configmap foo-backup -n ns backup_for_migration=up1": (0, "", "")
    }
    ctl = DummyCtl(cmds)
    mcs.backup_resource(ctl, "configmap", "foo", "ns", "bg_migration", "up1")
    # Ensure backup, label removal, label addition happen
    calls = ctl.calls
    assert any("kubectl label configmap foo-backup -n ns bg_migration-" in c for c in calls)
    assert any("kubectl label configmap foo-backup -n ns backup_for_migration=up1" in c for c in calls)

def test_backup_resource_skips_on_backup_fail():
    cmds = {
        # Resource exists
        "kubectl get configmap foo -n ns --ignore-not-found": (0, "NAME   DATA   AGE\nfoo    1      1d", ""),
        # But backup fails
        "kubectl get configmap foo -n ns -o json |": (1, "", "fail")
    }
    ctl = DummyCtl(cmds)
    # Should not raise, just skip backup
    mcs.backup_resource(ctl, "configmap", "foo", "ns", "bg_migration", "up1")
    # Calls should include get and backup attempt
    assert any("kubectl get configmap foo -n ns --ignore-not-found" in c for c in ctl.calls)
    assert any("kubectl get configmap foo -n ns -o json |" in c for c in ctl.calls)

def test_apply_yaml_to_cluster_success_and_failure():
    ctl_ok = DummyCtl({"kubectl apply": (0, "", "")})
    mcs.apply_yaml_to_cluster(ctl_ok, {"metadata": {"name": "foo"}})
    ctl_bad = DummyCtl({"kubectl apply": (1, "", "fail")})
    with pytest.raises(Exception):
        mcs.apply_yaml_to_cluster(ctl_bad, {"metadata": {"name": "foo"}})

def test_get_resources_by_label_success_and_failure():
    items = [{"metadata": {"name": "foo"}}, {"metadata": {"name": "bar"}}]
    ctl_ok = DummyCtl({"kubectl get": (0, yaml.dump({"items": items}), "")})
    assert mcs.get_resources_by_label(ctl_ok, "configmap", "ns", "k", "v") == ["foo", "bar"]
    ctl_fail = DummyCtl({"kubectl get": (1, "", "fail")})
    with pytest.raises(Exception):
        mcs.get_resources_by_label(ctl_fail, "configmap", "ns", "k", "v")

def test_label_and_collect_resources(monkeypatch):
    calls = []
    monkeypatch.setattr(mcs, "add_label", lambda *a, **k: calls.append(a))
    cms = ["cm1", "cm2"]
    secs = ["s1"]
    out_cms, out_secs = mcs.label_and_collect_resources(None, cms, secs, "ns", "k", "v")
    assert out_cms == cms
    assert out_secs == secs
    assert calls == [
        (None, "configmap", "cm1", "ns", "k", "v"),
        (None, "configmap", "cm2", "ns", "k", "v"),
        (None, "secret", "s1", "ns", "k", "v")
    ]

def test_migrate_executes_all_commands(monkeypatch):
    src = DummyCtl()
    # Updated dst to include existence checks
    dst_responses = {
        # Existence checks return non-empty output (resources exist)
        "kubectl get configmap cm1 -n ns1 --ignore-not-found": (0, "NAME   DATA   AGE\ncm1    1      1d", ""),
        "kubectl get secret s1 -n ns1 --ignore-not-found": (0, "NAME   TYPE     DATA   AGE\ns1     Opaque   1      1d", ""),
        # Backup commands
        "kubectl get configmap cm1 -n ns1 -o json |": (0, "", ""),
        "kubectl get secret s1 -n ns1 -o json |": (0, "", ""),
        "kubectl replace --force -n ns1 -f -": (0, "", ""),
        "kubectl label configmap cm1-backup -n ns1 bg_migration-": (0, "", ""),
        "kubectl label configmap cm1-backup -n ns1 backup_for_migration=up1": (0, "", ""),
        "kubectl label secret s1-backup -n ns1 bg_migration-": (0, "", ""),
        "kubectl label secret s1-backup -n ns1 backup_for_migration=up1": (0, "", ""),
        "kubectl apply -f -": (0, "", "")
    }
    dst = DummyCtl(dst_responses)
    monkeypatch.setattr(
        mcs,
        "fetch_resource_yaml",
        lambda ctl, t, n, ns, logger=None: {"metadata": {"name": n}}
    )
    mcs.migrate(
        src, dst,
        configmap_names=["cm1"], secret_names=["s1"],
        label_key="bg_migration", upgrade_id="up1", namespace="ns1"
    )
    calls = dst.calls
    assert any("kubectl get configmap cm1 -n ns1 --ignore-not-found" in c for c in calls)
    assert any("kubectl get configmap cm1 -n ns1 -o json |" in c for c in calls)
    assert any("kubectl label configmap cm1-backup -n ns1 backup_for_migration=up1" in c for c in calls)
    assert any("kubectl label configmap cm1-backup -n ns1 bg_migration-" in c for c in calls)

    assert any("kubectl get secret s1 -n ns1 --ignore-not-found" in c for c in calls)
    assert any("kubectl get secret s1 -n ns1 -o json |" in c for c in calls)
    assert any("kubectl label secret s1-backup -n ns1 backup_for_migration=up1" in c for c in calls)
    assert any("kubectl label secret s1-backup -n ns1 bg_migration-" in c for c in calls)


def test_migrate_with_labels_delegates(monkeypatch):
    calls = {}
    def fake_migrate(src, dst, cms, secs, label_key, upgrade_id=None, namespace=None, logger=None):
        calls["cms"] = cms
        calls["secs"] = secs
        calls["label_key"] = label_key
        calls["upgrade_id"] = upgrade_id
        calls["namespace"] = namespace
    monkeypatch.setattr(mcs, "migrate", fake_migrate)
    monkeypatch.setattr(
        mcs,
        "get_resources_by_label",
        lambda ctl, t, ns, lk, lv, logger=None: ["foo"] if t == "configmap" else ["bar"]
    )
    src = DummyCtl()
    dst = DummyCtl()
    mcs.migrate_with_labels(src, dst, label_key="bg_migration", upgrade_id="upx", namespace="NMX")
    assert calls["cms"] == ["foo"]
    assert calls["secs"] == ["bar"]
    assert calls["label_key"] == "bg_migration"
    assert calls["upgrade_id"] == "upx"
    assert calls["namespace"] == "NMX"

def test_rollback_migration_with_labels(monkeypatch):
    cm_items = [{"metadata": {"name": "cm1-backup"}}]
    sec_items = [{"metadata": {"name": "s1-backup"}}]
    responses = {
        "kubectl get configmap -n NMX -l backup_for_migration=UPX -o json": (0, yaml.dump({"items": cm_items}), ""),
        "kubectl get secret -n NMX -l backup_for_migration=UPX -o json": (0, yaml.dump({"items": sec_items}), ""),
        "kubectl get configmap cm1-backup -n NMX -o yaml": (0, yaml.dump({
            "metadata": {"name": "cm1-backup", "labels": {"backup_for_migration": "UPX"}}
        }), ""),
        "kubectl get secret s1-backup -n NMX -o yaml": (0, yaml.dump({
            "metadata": {"name": "s1-backup", "labels": {"backup_for_migration": "UPX"}}
        }), ""),
        "kubectl apply -f - <<EOF": (0, "", ""),
        "kubectl label configmap cm1-backup -n NMX backup_for_migration-": (0, "", ""),
        "kubectl label secret s1-backup -n NMX backup_for_migration-": (0, "", "")
    }
    ctl = DummyCtl(responses)
    mcs.rollback_migration_with_labels(ctl, upgrade_id="UPX", namespace="NMX")
    assert any("kubectl delete configmap cm1" in c for c in ctl.calls)
    assert any("kubectl delete secret s1" in c for c in ctl.calls)
    assert any("kubectl get configmap cm1-backup" in c for c in ctl.calls)
    assert any("kubectl apply -f - <<EOF" in c for c in ctl.calls)
    assert any("kubectl label configmap cm1-backup -n NMX backup_for_migration-" in c for c in ctl.calls)
    assert any("kubectl label secret s1-backup -n NMX backup_for_migration-" in c for c in ctl.calls)

def test_backup_resource_skips_when_resource_not_exists():
    """Test that backup_resource skips backup when resource doesn't exist."""
    # Mock kubectl get with --ignore-not-found returning empty output (resource doesn't exist)
    cmds = {
        "kubectl get configmap foo -n ns --ignore-not-found": (0, "", "")  # Empty stdout means resource doesn't exist
    }
    ctl = DummyCtl(cmds)

    # This should not raise an exception and should skip the backup
    mcs.backup_resource(ctl, "configmap", "foo", "ns", "bg_migration", "up1")

    # Verify that only the existence check was called, no backup/labeling commands
    calls = ctl.calls
    assert len(calls) == 1
    assert "kubectl get configmap foo -n ns --ignore-not-found" in calls[0]

    # Ensure backup and labeling commands were NOT called
    assert not any("kubectl replace --force" in c for c in calls)
    assert not any("kubectl label" in c for c in calls)

def test_migrate_with_some_resources_missing(monkeypatch):
    src_responses = {
        "kubectl get configmap cm1 -n ns --ignore-not-found": (0, "NAME   DATA   AGE\ncm1    1      1d", ""),
        "kubectl get configmap cm2 -n ns --ignore-not-found": (0, "", ""),  # cm2 does not exist
        "kubectl get secret s1 -n ns --ignore-not-found": (0, "NAME   TYPE     DATA   AGE\ns1     Opaque   1      1d", ""),
        "kubectl get secret s2 -n ns --ignore-not-found": (0, "", ""),  # s2 does not exist
    }
    dst_responses = {
        # Existence checks for destination
        "kubectl get configmap cm1 -n ns --ignore-not-found": (0, "NAME   DATA   AGE\ncm1    1      1d", ""),
        "kubectl get configmap cm2 -n ns --ignore-not-found": (0, "", ""),
        "kubectl get secret s1 -n ns --ignore-not-found": (0, "NAME   TYPE     DATA   AGE\ns1     Opaque   1      1d", ""),
        "kubectl get secret s2 -n ns --ignore-not-found": (0, "", ""),
        # Backup and label commands for existing resources
        "kubectl get configmap cm1 -n ns -o json |": (0, "", ""),
        "kubectl get secret s1 -n ns -o json |": (0, "", ""),
        "kubectl replace --force -n ns -f -": (0, "", ""),
        "kubectl label configmap cm1-backup -n ns bg_migration-": (0, "", ""),
        "kubectl label configmap cm1-backup -n ns backup_for_migration=upx": (0, "", ""),
        "kubectl label secret s1-backup -n ns bg_migration-": (0, "", ""),
        "kubectl label secret s1-backup -n ns backup_for_migration=upx": (0, "", ""),
        "kubectl apply -f -": (0, "", "")
    }
    src = DummyCtl(src_responses)
    dst = DummyCtl(dst_responses)

    def fake_fetch_resource_yaml(ctl, t, n, ns, logger=None):
        # Return resource for cm1 and s1, None for missing cm2 and s2
        if n in ["cm1", "s1"]:
            return {"metadata": {"name": n}}
        else:
            return None

    monkeypatch.setattr(mcs, "fetch_resource_yaml", fake_fetch_resource_yaml)

    mcs.migrate(
        src, dst,
        configmap_names=["cm1", "cm2"],
        secret_names=["s1", "s2"],
        label_key="bg_migration",
        upgrade_id="upx",
        namespace="ns"
    )

    calls = dst.calls
    # Check that backup and label commands were called for existing resources only
    assert any("kubectl get configmap cm1 -n ns --ignore-not-found" in c for c in calls)
    assert any("kubectl get secret s1 -n ns --ignore-not-found" in c for c in calls)
    assert any("kubectl get configmap cm1 -n ns -o json |" in c for c in calls)
    assert any("kubectl get secret s1 -n ns -o json |" in c for c in calls)
    assert any("kubectl label configmap cm1-backup -n ns backup_for_migration=upx" in c for c in calls)
    assert any("kubectl label secret s1-backup -n ns backup_for_migration=upx" in c for c in calls)
    # No calls related to cm2 or s2 backup or labeling
    assert not any("cm2-backup" in c for c in calls)
    assert not any("s2-backup" in c for c in calls)