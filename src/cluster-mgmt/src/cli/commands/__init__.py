import abc
import datetime
import json
import logging
import os
import pathlib
import shlex
import shutil
import stat
import subprocess
import sys
import tarfile
import time
import tempfile
import typing

import common
import jinja2
from common import (
    DockerImage,
    ECR_URL,
    IMAGES_SUBPATH,
)

ROOT_LOGGER_NAME = "cs_cluster"

handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.INFO)

fmt = '%(asctime)s %(levelname)s: %(message)s'
datefmt = '%Y-%m-%dT%H:%M:%SZ'
formatter = logging.Formatter(fmt, datefmt=datefmt)
formatter.converter = time.gmtime
handler.setFormatter(formatter)

logger = logging.getLogger("cs_cluster.init")
logger.setLevel(logging.INFO)
logger.handlers = []
logger.addHandler(handler)
APP_ROOT = str(pathlib.Path(__file__).parent.parent.joinpath("apps").absolute())


class Step(abc.ABC):
    @abc.abstractmethod
    def render(self, target_dir: pathlib.Path) -> str:
        """
        Render a step into a target_dir. Returns the name of the executable
        used to run the step relative to the target_dir
        """
        pass


class ImageStep(Step):
    def __init__(self, image: DockerImage, image_path: pathlib.Path, pull_from_ecr=False):
        self._name = image.format_name
        self.image_path = image_path
        self.image: DockerImage = image
        self._skip_push = False
        self._prune_all = False
        self._pull_from_ecr = pull_from_ecr
        self._load_mgmt_nodes = False
        self._load_all_nodes = False
        self._cleanup_after_push = False
        self._tag_latest = False
        self._load_nodes = None
        self._step_file = f"load-image-{self._name}.sh"

    @classmethod
    def from_file(cls, path: pathlib.Path) -> 'ImageStep':
        """
        Parameters:
            path: Must be a path to a tar/gz image file. .docker extension OK
        """
        with tarfile.open(path, mode="r") as t:
            with t.extractfile("manifest.json") as f:
                image = DockerImage.of(json.loads(f.read())[0].get("RepoTags", [""])[0])
            for member in t.getmembers():
                if member.name.endswith('json'):
                    manifest = t.extractfile(member)
                    manifest_data = json.load(manifest)
                    if 'config' in manifest_data and 'Labels' in manifest_data['config']:
                        image.set_labels(manifest_data['config']['Labels'])
                        break
        return cls(image, path)

    @classmethod
    def from_pullable_image(cls, ctx, image: DockerImage, target_dir: pathlib.Path = None,
                            pull_image: bool = True, gzip: bool = True, with_dind: bool = False) -> 'ImageStep':

        image_dir = pathlib.Path(tempfile.gettempdir())
        if target_dir is not None:
            target_dir.mkdir(exist_ok=True, parents=True)
            image_dir = target_dir
        tar_image_path = image_dir.joinpath(image.tar_name)
        zip_image_path = image_dir.joinpath(image.tgz_name)
        platform_metadata = image_dir.joinpath(".platform_meta_" + image.format_name)

        target_file = zip_image_path if gzip else tar_image_path
        hidden_target_file = target_file.parent / ".." / target_file.name

        if not pull_image and image.repo.startswith(ECR_URL):
            # Skip pull image if it is from AWS ECR instance
            # Only cbcore internal deploy flow invokes this branch
            logger.info(f"skip pulling_image {image} since it is in ECR")

            if target_file.is_file():
                logger.info(f"Hiding {target_file} from this deployment to avoid unnecessary copy.")
                # Hide a previous export of this image, its not needed.
                target_file.rename(hidden_target_file)

            return cls(image, None, pull_from_ecr=True)

        if hidden_target_file.is_file():
            logger.info(f"Unhiding {hidden_target_file} from previous export.")
            # Unhidden previously hidden export of this image.
            hidden_target_file.rename(target_file)

        needs_export = False
        if not os.path.exists(target_file):
            logger.info(f"{target_file} not exist, needs export")
            needs_export = True
        # only skip export tar image if for sure it's the same platform
        elif not os.path.exists(platform_metadata) or platform_metadata.read_text() != ctx.build_platform():
            logger.info(f"not able to validate platform, needs export")
            needs_export = True

        if needs_export:
            image.export(ctx, tar_image_path, zip_image_path, platform_metadata, gzip=gzip,
                         with_dind=with_dind or image.dind)
        # ensure label parsing
        return cls.from_file(target_file)

    @property
    def remote_image(self) -> DockerImage:
        return DockerImage.of(common.REGISTRY_URL + "/" + self.image.short_name)

    def skip_push(self) -> 'ImageStep':
        self._skip_push = True
        return self

    # tag with latest for tool images like "alpine-kubectl/containerd"
    def tag_latest(self) -> 'ImageStep':
        self._tag_latest = True
        return self

    def prune_all(self) -> 'ImageStep':
        self._prune_all = True
        return self

    def load_mgmt_nodes(self) -> 'ImageStep':
        self._load_mgmt_nodes = True
        return self

    def load_all_nodes(self, nodes: typing.Optional[list] = None) -> 'ImageStep':
        self._load_all_nodes = True
        self._load_nodes = nodes
        return self

    def cleanup_after_push(self) -> 'ImageStep':
        self._cleanup_after_push = True
        return self

    def render(self, target_dir: pathlib.Path) -> str:
        target_img_file = target_dir.joinpath(self.image.tgz_name)
        image_path = target_img_file.name
        img_shared_file = target_dir.parent.joinpath(IMAGES_SUBPATH, self.image.tgz_name)
        if not self._pull_from_ecr and not os.path.exists(target_img_file):
            if img_shared_file == self.image_path:
                logger.info(f"image is in shared path {img_shared_file}. Skip copy")
                image_path = f"../{IMAGES_SUBPATH}/{image_path}"
            else:
                logger.info(f"{target_img_file} not exist, copying")
                shutil.copyfile(self.image_path, target_img_file)
        image_sh = target_dir.joinpath(self._step_file)
        image_vars = {
            "image": self.image,
            "tag": self.image.tag,
            "image_path": image_path,
            "image_name": str(self.image.short_name),
            "repo_name": str(self.image.short_repo),
            "skip_push": self._skip_push,
            "prune_all": self._prune_all,
            "tag_latest": self._tag_latest,
            "pull_from_ecr": self._pull_from_ecr,
            "load_mgmt_nodes": self._load_mgmt_nodes,
            "load_all_nodes": self._load_all_nodes,
            "cleanup_after_push": self._cleanup_after_push,
            "load_nodes": ",".join(self._load_nodes) if self._load_nodes else "",
        }
        env = jinja2.Environment(loader=jinja2.FileSystemLoader([f"{APP_ROOT}/common"]))
        image_sh.write_text(env.get_template("load-image.sh.jinja2").render(image_vars))
        image_sh.chmod(image_sh.stat().st_mode | stat.S_IEXEC)
        return self._step_file


class ScriptStep(Step):
    """Renders a jinja template assuming that the output is an executable file."""

    def __init__(self, script_path: typing.Union[pathlib.Path, str], variables: dict,
                 name: typing.Optional[str] = None):
        self._path = pathlib.Path(script_path)
        if name:
            self.name = name
        else:
            self.name = self._path.name.replace(".jinja2", "")
        self._variables = variables

    def render(self, target_dir: pathlib.Path) -> str:
        target_file = target_dir.joinpath(self.name)
        if self._path.name.endswith(".jinja2"):
            env = jinja2.Environment(
                loader=jinja2.FileSystemLoader([self._path.parent]),
                # Use "{= =}" for comments since the default "{# #}" can be used for arrays.
                comment_start_string='{=',
                comment_end_string='=}'
            )
            target_file.write_text(env.get_template(self._path.name).render(self._variables))
        else:
            shutil.copy2(self._path, target_file)
        target_file.chmod(target_file.stat().st_mode | stat.S_IEXEC)
        return self.name


class StepsManifest(Step):
    def __init__(self, name: str):
        self._name = name
        self._files = []
        self._steps = []
        # A step to skip the rest of steps in this manifest. For example, cilium
        # might be installed already, so we should skip the rest of the steps.
        self._skip_step = None
        self._step_file = f"apply-{self._name}.sh"

    def add_step(self, step: Step) -> 'StepsManifest':
        self._steps.append(step)
        return self

    def set_skip_step(self, step: Step) -> 'StepsManifest':
        self._skip_step = step
        return self

    def include_files(self, *files):
        self._files.extend(files)

    def render(self, target_dir: pathlib.Path) -> str:
        target_dir.mkdir(exist_ok=True, parents=True)
        for file in self._files:
            if os.path.basename(file) == "*":
                parent_dir = os.path.dirname(file)
                subprocess.check_call(f"cp {parent_dir}/* {target_dir}", shell=True)
                continue

            path = pathlib.Path(file)
            if path.is_file():
                target_path = target_dir / path.name
                shutil.copyfile(path, target_path)
            elif path.is_dir():
                subprocess.check_call(shlex.split(f"cp -rf {path} {target_dir}"))
                # in 3.8+, we'd use:
                # shutil.copytree(path, target_dir / path.name, dirs_exist_ok=True)
            else:
                raise ValueError(f"file included was not found: {file}")
        subprocess.call(f"chmod +x {target_dir}/*.sh", shell=True, stderr=subprocess.DEVNULL)

        step_scripts = []
        images = []
        for step in self._steps:
            step_scripts.append(step.render(target_dir))
            if isinstance(step, ImageStep):
                images.append(step.image.short_name)
        env = jinja2.Environment(loader=jinja2.FileSystemLoader([f"{APP_ROOT}/common"]))
        apply = target_dir.joinpath(self._step_file)
        apply.write_text(
            env.get_template("apply-steps.sh.jinja2")
            .render({
                "step_scripts": step_scripts,
                "skip_step": self._skip_step.render(target_dir) if self._skip_step else None
            }))
        apply.chmod(apply.stat().st_mode | stat.S_IEXEC)

        # Write all related images.
        all_images = target_dir.joinpath("all_images.txt")
        all_images.write_text("\n".join(images))
        return self._step_file


class CbcoreManifest:
    def __init__(self, path, image: DockerImage):
        self.path = path
        self.image = image


class ClusterManifest:
    """Parent manifest containing component manifests"""

    def __init__(self,
                 version: str,
                 component_paths: typing.List[pathlib.Path]):
        self.version: str = version
        self.component_paths = component_paths

    def write_manifest(self, pkg_path: pathlib.Path, internal=False):
        """ Write manifest to {pkg_path}"""
        with open(f"{pkg_path}/manifest.json", "w") as manifest_file:
            manifest_file.write(json.dumps(self.as_dict(), indent=2))
        # this could probably be done in a more elegant way - basically, almost
        # all the scripts expect these files to be in their parent dir.
        shutil.copy2(f"{APP_ROOT}/common/pkg-common.sh", pkg_path)
        shutil.copytree(f"{APP_ROOT}/common/pkg-functions", f"{pkg_path}/pkg-functions", dirs_exist_ok=True)
        shutil.copy2(f"{APP_ROOT}/common/cluster-pkg-properties.yaml.template", pkg_path)
        if internal:
            shutil.copy2(f"{APP_ROOT}/common/internal-pkg-properties.yaml", pkg_path)
        for src in pathlib.Path(f"{APP_ROOT}/csadm").glob("*"):
            dst = pkg_path / src.name
            if src.is_file():
                shutil.copy2(src, dst)
            elif src.is_dir():
                if dst.exists():
                    shutil.rmtree(dst)
                shutil.copytree(src, dst)
        shutil.copy2(common.models.__file__, pkg_path / "csadm/models.py")

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "componentPaths": [str(m) for m in self.component_paths]
        }

    @staticmethod
    def write_partial_manifest(component_paths: typing.List[pathlib.Path], pkg_path: pathlib.Path, internal=False):
        ClusterManifest(
            f"{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S.%fZ')}-partial",
            component_paths
        ).write_manifest(pkg_path, internal)
