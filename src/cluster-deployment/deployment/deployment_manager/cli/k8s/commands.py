import difflib
import logging
import pathlib
import tempfile

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.common.models import K_SYSTEM_ADMIN_PASSWORD, K_AWS_CREDENTIAL_KEY, K_AWS_CREDENTIAL_SECRET, \
    K_AWS_ACCOUNT_ID, K_AWS_REGION, well_known_secrets
from deployment_manager.db import device_props as props
from deployment_manager.db.models import Cluster
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.kubernetes.interface import KubernetesCtl
from deployment_manager.tools.kubernetes.utils import get_k8s_lead_mgmt_node
from deployment_manager.tools.network_config_tool import NetworkConfigTool
from deployment_manager.tools.secrets import create_secrets_provider
from deployment_manager.tools.utils import prompt_confirm

logger = logging.getLogger(__name__)


class UpdateNetworkConfig(SubCommandABC):
    """ Update network_config.json on k8s deploy node """

    name = "update_network_config"

    def construct(self):
        self.parser.add_argument(
            "--cluster",
            dest="cluster_name",
            type=str,
            default=None,
            help="(optional) Name of the blue/green cluster. The primary cluster will be used if not specified."
        )
        self.add_arg_noconfirm()

    def run(self, args):
        cluster_name = args.cluster_name
        if not cluster_name:
            cluster = Cluster.objects.get(profile__name=self.profile, is_primary=True)
            cluster_name = cluster.name

        nwt = NetworkConfigTool.for_profile(self.profile)
        k8s_cfg = nwt.generate_nw_config_for_k8s(cluster_name)

        dnode = get_k8s_lead_mgmt_node(self.profile, cluster_name=cluster_name)
        if not dnode:
            logger.error("Unable to determine K8S deploy node for cluster %s", cluster_name)
            return 1
        user = dnode.get_prop(props.prop_management_credentials_user)
        password = dnode.get_prop(props.prop_management_credentials_password)
        logger.info("Using cluster %s with node %s", cluster_name, dnode.name)

        # get the existing network config from the K8S deploy node to show user a diff
        with KubernetesCtl(self.profile, dnode.name, user, password) as kctl:
            remote_nw_doc = kctl.get_network_config()

        logger.info(f"Config generated at {k8s_cfg}")
        if remote_nw_doc:
            new_doc = pathlib.Path(k8s_cfg).read_text()
            diff = "\n".join(difflib.unified_diff(
                remote_nw_doc.split("\n"), new_doc.split("\n"),
                "REMOTE", "NEW",
            ))
            if diff:
                logger.info(f"Diff existing/proposed doc:\n{diff}")
            else:
                logger.info("No changes detected in the network config, nothing to upload")
                return 0
        else:
            logger.info("No existing network config found on K8S deploy node, will upload the new one")

        if not args.noconfirm and not prompt_confirm("Upload to K8S deploy node?"):
            logger.info("Aborted upload to K8S deploy node")
            return 0

        with KubernetesCtl(self.profile, dnode.name, user, password) as kctl:
            kctl.upload_network_config(k8s_cfg)
        logger.info("Config uploaded to K8S deploy node")

        return 0


class UpdateSystemSecret(SubCommandABC):
    """Set the k8s system-credential secret used for ssh logins to the system CM"""

    name = "update_system_secret"

    def construct(self):
        pass

    def run(self, args):
        cfg = ConfGen(self.profile)

        provider = create_secrets_provider(cfg.parse_profile())
        non_default_admin_password = provider.get(K_SYSTEM_ADMIN_PASSWORD)
        if non_default_admin_password:
            dnode = get_k8s_lead_mgmt_node(self.profile)
            if dnode is not None:
                user = dnode.get_prop(props.prop_management_credentials_user)
                password = dnode.get_prop(props.prop_management_credentials_password)
                with KubernetesCtl(self.profile, dnode.name, user, password) as kctl:
                    with tempfile.NamedTemporaryFile(mode="w") as tf:
                        tf.write(non_default_admin_password)
                        tf.flush()
                        ret = kctl.create_prometheus_system_credential_secret("system-credential", tf.name)
                        if ret:
                            logger.error("Failed to update k8s system-credential secret: "
                                         "could not successfully execute command on any management node.")
                        else:
                            logger.info("Updated k8s system-credential secret.")
            else:
                logger.error("Failed to update k8s system-credential secret: "
                             "unable to determine k8s deployment node")
        else:
            logger.error(
                "Failed to update k8s system-credential secret: password not present in the secrets provider."
            )

        return 1


class UpdateAwsEcrCredential(SubCommandABC):
    """Set the k8s aws-ecr-credential secret used for ECR token refresh"""

    name = "update_aws_ecr_credential"

    def construct(self):
        pass

    def run(self, args):
        cfg = ConfGen(self.profile)

        provider = create_secrets_provider(cfg.parse_profile())
        aws_key = provider.get(K_AWS_CREDENTIAL_KEY)
        aws_secret = provider.get(K_AWS_CREDENTIAL_SECRET)
        aws_account_id = provider.get(K_AWS_ACCOUNT_ID)
        aws_region = provider.get(K_AWS_REGION)
        if not aws_account_id:
            aws_account_id = well_known_secrets().get(K_AWS_ACCOUNT_ID)
        if not aws_region:
            aws_region = well_known_secrets().get(K_AWS_REGION)
        logger.info(f"Updating k8s aws-ecr-credential secret with account id {aws_account_id} and region {aws_region}.")
        if aws_key and aws_secret:
            dnode = get_k8s_lead_mgmt_node(self.profile)
            if dnode is not None:
                user = dnode.get_prop(props.prop_management_credentials_user)
                password = dnode.get_prop(props.prop_management_credentials_password)
                with KubernetesCtl(self.profile, dnode.name, user, password) as kctl:
                    with tempfile.NamedTemporaryFile(mode="w") as tf_key:
                        tf_key.write(aws_key)
                        tf_key.flush()
                        with tempfile.NamedTemporaryFile(mode="w") as tf_secret:
                            tf_secret.write(aws_secret)
                            tf_secret.flush()
                            ret = kctl.create_aws_ecr_credential_secret(
                                "aws-ecr-credential", tf_key.name, tf_secret.name, aws_account_id, aws_region)
                            if ret:
                                logger.error("Failed to update k8s aws-ecr-credential secret: "
                                             "could not successfully execute command on any management node.")
                            else:
                                logger.info("Updated k8s aws-ecr-credential secret.")
                                return 0
            else:
                logger.error("Failed to update k8s aws-ecr-credential secret: "
                             "unable to determine k8s deployment node")
        else:
            not_present = []
            if not aws_key:
                not_present.append(K_AWS_CREDENTIAL_KEY)
            if not aws_secret:
                not_present.append(K_AWS_CREDENTIAL_SECRET)
            logger.error(
                f"Failed to update k8s aws-ecr-credential secret: credential(s) {' and '.join(not_present)} are not present in the secrets provider."
            )

        return 1
