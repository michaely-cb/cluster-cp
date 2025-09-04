import logging
import yaml
from deployment_manager.tools.kubernetes.interface import KubernetesCtl
from typing import List, Tuple, Optional

def strip_k8s_metadata(resource: dict, logger=logging.getLogger(__name__)) -> dict:
    logger.debug(f"Stripping metadata from resource: {resource.get('metadata', {}).get('name')}")
    metadata = resource.get("metadata", {})
    for field in ["resourceVersion", "uid", "creationTimestamp", "managedFields", "selfLink"]:
        metadata.pop(field, None)
    return resource

def check_resource_exists(kctl, resource_type: str, name: str, namespace: str, logger=logging.getLogger(__name__)) -> bool:
    """Helper function to check if a resource exists consistently"""
    logger.debug(f"Checking if {resource_type} '{name}' exists in namespace '{namespace}'")
    ret, stdout, err = kctl.run(
        f"kubectl get {resource_type} {name} -n {namespace} --ignore-not-found"
    )
    exists = bool(stdout.strip())
    if not exists:
        logger.debug(f"{resource_type} '{name}' does not exist in namespace '{namespace}'")
    return exists

def add_label(kctl, resource_type: str, name: str, namespace: str, label_key: str, label_value: str, logger=logging.getLogger(__name__)):
    logger.info(f"Ensuring label on {resource_type} '{name}' in namespace '{namespace}': {label_key}={label_value}")

    if not check_resource_exists(kctl, resource_type, name, namespace, logger):
        logger.warning(f"⚠️ {resource_type} '{name}' does not exist in namespace '{namespace}', skipping labeling")
        return

    ret, resource_data, _ = kctl.run(
        f"kubectl get {resource_type} {name} -n {namespace} -o json"
    )
    if ret != 0:
        logger.warning(f"⚠️ Failed to retrieve {resource_type} {name} for labeling, skipping")
        return

    resource_json = yaml.safe_load(resource_data)
    labels = resource_json.get('metadata', {}).get('labels', {})
    if labels.get(label_key) != label_value:
        logger.debug(f"Current labels: {labels}")
        ret, _, err = kctl.run(
            f"kubectl label {resource_type} {name} -n {namespace} {label_key}={label_value} --overwrite"
        )
        if ret != 0:
            logger.warning(f"⚠️ Error labeling {resource_type} {name}: {err}, skipping")
            return
        logger.info(f"Labeled {resource_type} '{name}' with {label_key}={label_value}")
    else:
        logger.debug(f"Label already present on {resource_type} '{name}'")

def fetch_resource_yaml(kctl, resource_type: str, name: str, namespace: str, logger=logging.getLogger(__name__)) -> Optional[dict]:
    logger.info(f"Fetching YAML for {resource_type} '{name}' in namespace '{namespace}'")

    if not check_resource_exists(kctl, resource_type, name, namespace, logger):
        logger.warning(f"⚠️ {resource_type} '{name}' does not exist in namespace '{namespace}', skipping fetch")
        return None

    ret, data, err = kctl.run(
        f"kubectl get {resource_type} {name} -n {namespace} -o yaml"
    )
    if ret != 0:
        logger.warning(f"⚠️ Failed to fetch {resource_type} {name}: {err}, skipping")
        return None

    resource = yaml.safe_load(data)
    logger.debug(f"Fetched resource: {resource}")
    return resource

def backup_resource(kctl, resource_type: str, name: str, namespace: str, label_key, upgrade_id: str, logger=logging.getLogger(__name__)):
    backup_name = f"{name}-backup"
    backup_label_key = "backup_for_migration"
    label_value = upgrade_id

    # Check if the resource exists first
    logger.info(f"🔍 Checking if {resource_type} '{name}' exists in namespace '{namespace}'")
    if not check_resource_exists(kctl, resource_type, name, namespace, logger):
        logger.warning(f"⚠️ {resource_type} '{name}' does not exist in namespace '{namespace}', skipping backup")
        return

    logger.info(f"🔒 Backing up {resource_type} '{name}' to '{backup_name}'")
    ret, _, err = kctl.run(
        f"kubectl get {resource_type} {name} -n {namespace} -o json | "
        f"jq '.metadata.name = \"{backup_name}\"' | "
        f"kubectl replace --force -n {namespace} -f -"
    )
    if ret != 0:
        logger.warning(f"⚠️ Failed to backup {resource_type} {name}: {err}, skipping")
        return

    logger.info(f"🏷️ Labeling backup {resource_type} '{backup_name}' with {backup_label_key}={label_value}")
    ret, _, err = kctl.run(
        f"kubectl label {resource_type} {backup_name} -n {namespace} {label_key}-"
    )
    if ret != 0:
        logger.warning(f"Failed to remove unused label for {resource_type} {backup_name}: {err}")

    ret, _, err = kctl.run(
        f"kubectl label {resource_type} {backup_name} -n {namespace} {backup_label_key}={label_value} --overwrite"
    )
    if ret != 0:
        logger.warning(f"⚠️ Failed to label backup {resource_type} {backup_name}: {err}")

def apply_yaml_to_cluster(kctl, resource: dict, logger=logging.getLogger(__name__)):
    name = resource.get('metadata', {}).get('name')
    logger.info(f"Applying resource '{name}' to destination cluster")
    yaml_str = yaml.dump(resource)
    ret, _, err = kctl.run(f"kubectl apply -f - <<EOF\n{yaml_str}\nEOF")
    if ret != 0:
        logger.error(f"Failed to apply resource {name}: {err}")
        raise Exception(f"Failed to apply resource {name}: {err}")
    logger.debug(f"Resource '{name}' applied successfully")

def get_resources_by_label(kctl, resource_type: str, namespace: str, label_key: str, label_value: str, logger=logging.getLogger(__name__)) -> list:
    logger.info(f"Listing {resource_type}s in namespace '{namespace}' with label {label_key}={label_value}")
    ret, data, _ = kctl.run(
        f"kubectl get {resource_type} -n {namespace} -l {label_key}={label_value} -o json"
    )
    if ret != 0:
        logger.error(f"Failed to list {resource_type}s with label {label_key}={label_value}")
        raise Exception(f"Failed to list {resource_type}s with label {label_key}={label_value}")
    parsed = yaml.safe_load(data)
    names = [item['metadata']['name'] for item in parsed.get('items', [])]
    logger.debug(f"Found {len(names)} {resource_type}(s): {names}")
    return names

def label_and_collect_resources(
        kctl,
        configmap_names: list,
        secret_names: list,
        namespace: str,
        label_key: str,
        label_value: str,
        logger=logging.getLogger(__name__)
) -> Tuple[list, list]:
    labeled_cms = []
    labeled_secrets = []
    for cm in configmap_names:
        logger.info(f"🔖 Labeling source ConfigMap '{cm}' with {label_key}={label_value}")
        add_label(kctl, "configmap", cm, namespace, label_key, label_value, logger=logger)
        labeled_cms.append(cm)
    for secret in secret_names:
        logger.info(f"🔖 Labeling source Secret '{secret}' with {label_key}={label_value}")
        add_label(kctl, "secret", secret, namespace, label_key, label_value, logger=logger)
        labeled_secrets.append(secret)
    return labeled_cms, labeled_secrets

def migrate(
        src_k8s: KubernetesCtl,
        dest_k8s: KubernetesCtl,
        configmap_names: list,
        secret_names: list,
        label_key: str,
        upgrade_id: str,
        namespace: str = "prometheus",
        logger=logging.getLogger(__name__)
):
    label_value = upgrade_id
    logger.info("🔁 Starting migration...")

    for cm in configmap_names:
        logger.info(f"📦 Migrating ConfigMap: {cm}")
        backup_resource(dest_k8s, "configmap", cm, namespace, label_key, upgrade_id, logger=logger)
        resource = fetch_resource_yaml(src_k8s, "configmap", cm, namespace, logger=logger)
        if resource is None:
            logger.warning(f"⚠️ Skipping migration for ConfigMap {cm} because it does not exist in source cluster")
            continue
        strip_k8s_metadata(resource, logger=logger)
        apply_yaml_to_cluster(dest_k8s, resource, logger=logger)
        ret, _, err = dest_k8s.run(
            f"kubectl label configmap {cm} -n {namespace} {label_key}={label_value} --overwrite"
        )
        if ret != 0:
            logger.warning(f"⚠️ Failed to label ConfigMap {cm} in destination: {err}")
        else:
            logger.info(f"✅ Migrated & labeled ConfigMap: {cm}")

    for secret in secret_names:
        logger.info(f"📦 Migrating Secret: {secret}")
        backup_resource(dest_k8s, "secret", secret, namespace, label_key, upgrade_id, logger=logger)
        resource = fetch_resource_yaml(src_k8s, "secret", secret, namespace, logger=logger)
        if resource is None:
            logger.warning(f"⚠️ Skipping migration for Secret {secret} because it does not exist in source cluster")
            continue
        strip_k8s_metadata(resource, logger=logger)
        apply_yaml_to_cluster(dest_k8s, resource, logger=logger)
        ret, _, err = dest_k8s.run(
            f"kubectl label secret {secret} -n {namespace} {label_key}={label_value} --overwrite"
        )
        if ret != 0:
            logger.warning(f"⚠️ Failed to label Secret {secret} in destination: {err}")
        else:
            logger.info(f"✅ Migrated & labeled Secret: {secret}")

    logger.info("🎉 Migration complete")

def migrate_with_labels(
        src_k8s: KubernetesCtl,
        dest_k8s: KubernetesCtl,
        label_key,
        upgrade_id: str,
        namespace: str = "prometheus",
        logger=logging.getLogger(__name__)
):
    label_value = upgrade_id

    try:
        logger.info(f"Looking for resources with label {label_key}={label_value} in namespace '{namespace}'")
        configmap_names = get_resources_by_label(src_k8s, "configmap", namespace, label_key, label_value, logger=logger)
        secret_names = get_resources_by_label(src_k8s, "secret", namespace, label_key, label_value, logger=logger)
        if not configmap_names and not secret_names:
            logger.warning("⚠️ No labeled ConfigMaps or Secrets found for migration")
            return
        migrate(
            src_k8s,
            dest_k8s,
            configmap_names,
            secret_names,
            label_key,
            upgrade_id=upgrade_id,
            namespace=namespace,
            logger=logger
        )
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise

def rollback_migration_with_labels(
        dest_k8s: KubernetesCtl,
        upgrade_id: str,
        namespace: str = "prometheus",
        logger=logging.getLogger(__name__)
):
    backup_label_key = "backup_for_migration"
    backup_label_value = upgrade_id

    try:
        logger.info(f"🔄 Starting rollback for upgrade_id={upgrade_id} in namespace={namespace}")
        for resource_type in ["configmap", "secret"]:
            logger.info(f"🔍 Locating {resource_type}s labeled {backup_label_key}={backup_label_value}")
            try:
                names = get_resources_by_label(
                    dest_k8s, resource_type, namespace, backup_label_key, backup_label_value, logger=logger
                )
            except Exception as e:
                logger.warning(f"⚠️ Could not fetch {resource_type}s: {e}")
                continue

            for name in names:
                # Fix: Extract original name from backup name
                if name.endswith("-backup"):
                    original_name = name[:-7]  # Remove "-backup" suffix
                else:
                    logger.warning(f"⚠️ Backup name '{name}' doesn't follow expected pattern, skipping")
                    continue

                backup_name = name  # The name we found is already the backup name
                logger.info(f"🗑️ Deleting migrated {resource_type} '{original_name}'")
                dest_k8s.run(f"kubectl delete {resource_type} {original_name} -n {namespace} --ignore-not-found")
                logger.info(f"🔍 Fetching backup {resource_type} '{backup_name}' for restoration")
                ret, backup_yaml, err = dest_k8s.run(
                    f"kubectl get {resource_type} {backup_name} -n {namespace} -o yaml"
                )
                if ret != 0:
                    logger.warning(f"⚠️ Backup '{backup_name}' not found or failed to fetch: {err}")
                    continue
                obj = yaml.safe_load(backup_yaml)
                backup_labels = obj.get("metadata", {}).get("labels", {})
                if backup_labels.get(backup_label_key) != backup_label_value:
                    logger.warning(f"⚠️ Backup {resource_type} '{backup_name}' missing label {backup_label_key}={backup_label_value}, skipping.")
                    continue
                obj["metadata"]["name"] = original_name  # rename back to original
                strip_k8s_metadata(obj, logger=logger)
                yaml_str = yaml.dump(obj)
                logger.info(f"♻️ Restoring original {resource_type} '{original_name}' from backup")
                ret, _, err = dest_k8s.run(f"kubectl apply -f - <<EOF\n{yaml_str}\nEOF")
                if ret != 0:
                    logger.error(f"❌ Failed to restore {resource_type} '{original_name}': {err}")
                    continue
                logger.info(f"✅ Successfully restored {resource_type} '{original_name}' from backup")
                # Remove the backup label from the backup copy
                logger.info(f"🧹 Removing label {backup_label_key} from backup {resource_type} '{backup_name}'")
                ret, _, err = dest_k8s.run(
                    f"kubectl label {resource_type} {backup_name} -n {namespace} {backup_label_key}- --overwrite"
                )
                if ret != 0:
                    logger.warning(f"⚠️ Failed to remove label from {resource_type} '{backup_name}': {err}")
                else:
                    logger.info(f"✅ Removed label {backup_label_key} from backup {resource_type} '{backup_name}'")
        logger.info("🎉 Rollback completed")
    except Exception as e:
        logger.error(f"❌ Rollback failed: {e}")
        raise