import logging

from django.db import migrations

from deployment_manager.tools.config import ConfGen

logger = logging.getLogger(__name__)


def seed_cluster(apps, schema_editor):
    """
    For each profile, create a corresponding Cluster object with all the devices of that profile added to the cluster.

    Future workflows should associate devices with the primary cluster automatically, but this process will bootstrap.
    """

    Cluster = apps.get_model('db', 'Cluster')
    DeploymentProfile = apps.get_model('db', 'DeploymentProfile')

    # Check if there are any profiles to process
    profiles = DeploymentProfile.objects.all()
    if not profiles.exists():
        logger.info("No deployment profiles found during migration - skipping cluster seeding")
        return

    for profile in profiles:
        cg = ConfGen(profile=profile.name)
        doc = cg.parse_profile()
        cluster_name = doc.get("basic", {}).get("name", profile.name)
        domain = doc.get("basic", {}).get("domain", "cerebras.internal")

        mgmt_vip = doc.get("basic", {}).get("mgmt_network_int_config", {}).get("mgmt_vip", {}).get("vip", "")

        cluster, _ = Cluster.objects.get_or_create(
            name=cluster_name,
            profile=profile,
            defaults=dict(
                dns_name=f"{cluster_name}.{domain}",
                is_primary=True,
                management_vip=mgmt_vip,
                profile=profile,  # Ensure the cluster is linked to the profile
            )
        )


def set_single_cluster_primary(apps, schema_editor):
    """
    Set a cluster as primary if it's the only one for its profile.
    This ensures that profiles with a single cluster always have that cluster marked as primary.
    """
    Cluster = apps.get_model('db', 'Cluster')

    # Check if there are any clusters to process
    clusters = Cluster.objects.all()
    if not clusters.exists():
        logger.info("No clusters found during migration - skipping primary cluster setting")
        return

    # Get profiles with exactly one cluster
    profiles_with_clusters = {}
    for cluster in clusters:
        profile_id = cluster.profile_id
        if profile_id not in profiles_with_clusters:
            profiles_with_clusters[profile_id] = []
        profiles_with_clusters[profile_id].append(cluster)

    # For each profile with exactly one cluster, set that cluster as primary
    for profile_id, clusters in profiles_with_clusters.items():
        if len(clusters) == 1:
            single_cluster = clusters[0]
            if not single_cluster.is_primary:
                logger.info(f"Setting cluster '{single_cluster.name}' as primary for profile ID {profile_id}")
                single_cluster.is_primary = True
                single_cluster.save()


def associate_all_devices_with_cluster(apps, schema_editor):
    """
    Ensure all appropriate devices are associated with a cluster based on their profile.
    Only associates servers (SR) and systems (SY), excluding infra (IN) and usernodes (US).
    """
    Device = apps.get_model('db', 'Device')
    Cluster = apps.get_model('db', 'Cluster')
    ClusterDevice = apps.get_model('db', 'ClusterDevice')
    DeviceProp = apps.get_model('db', 'DeviceProperty')

    # Check if there are any devices to process
    devices = Device.objects.filter(device_type__in=("SR", "SY")).exclude(device_role__in=("IN", "US"))
    if not devices.exists():
        logger.info("No devices found during migration - skipping device-cluster association")
        return

    # Dictionary to cache primary clusters by profile to avoid repeated lookups
    profile_to_cluster = {}

    # Only associate servers (SR) and systems (SY)
    # Exclude infra (IN) and usernodes (US)
    for device in devices:
        # Skip devices already associated with any cluster
        if ClusterDevice.objects.filter(device=device).exists():
            logger.info(f"Device '{device.name}' already in ClusterDevice, skip recreate")
            continue

        # Get the device's profile directly
        profile = device.profile
        if not profile:
            logger.warning(f"Device '{device.name}' has no profile, skipping association")
            continue

        # Find or cache the appropriate cluster for this profile
        if profile.profile_id in profile_to_cluster:
            chosen_cluster = profile_to_cluster[profile.profile_id]
        else:
            try:
                chosen_cluster = Cluster.objects.get(profile=profile, is_primary=True)
                profile_to_cluster[profile.profile_id] = chosen_cluster
            except Cluster.DoesNotExist:
                logger.warning(f"Profile '{profile.name}' has no primary cluster, skipping association")
                continue

        # Check for kubernetes.controlplane property to determine controlplane status
        is_controlplane = False
        if device.device_type == "SR" and device.device_role == "MG":
            prop = DeviceProp.objects.filter(
                device=device,
                property_name="kubernetes",
                property_attribute="controlplane",
                property_value="True",
            ).first()
            # if current device has controlplane prop
            # or if current device is the only mgmt node, then default to controlplane node
            # or if current management node is root server, then it's almost certainly a controlplane node
            if (prop or
                ConfGen(profile=profile.name).get_root_server() == device.name or
                Device.objects.filter(
                    profile__name=profile.name,
                    device_type="SR",
                    device_role="MG"
                ).count() == 1):
                is_controlplane = True

        # Create new ClusterDevice association with the chosen cluster
        logger.info(
            f"Associating device '{device.name}' to cluster '{chosen_cluster.name}' "
            f"(profile: {profile.name}), controlplane: {is_controlplane}"
        )
        ClusterDevice.objects.create(
            device=device,
            cluster=chosen_cluster,
            controlplane=is_controlplane
        )


class Migration(migrations.Migration):
    dependencies = [
        ('db', '0006_init_cluster'),
    ]

    operations = [
        migrations.RunPython(seed_cluster, migrations.RunPython.noop),
        migrations.RunPython(set_single_cluster_primary, migrations.RunPython.noop),
        migrations.RunPython(associate_all_devices_with_cluster, migrations.RunPython.noop),
    ]
