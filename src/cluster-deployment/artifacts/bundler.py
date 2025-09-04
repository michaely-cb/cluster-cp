#!/usr/bin/env python3

import abc
import argparse
import hashlib
import inspect
import logging
import os
import pathlib
import shutil
import subprocess
import sys
import tarfile
import time
import typing
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from enum import Enum
from typing import List, Optional

import requests
import yaml

logger = logging.getLogger(__name__)

FILE_SERVER = "deploy-artifacts.cerebrassc.local"
PROTOCOL = "http"


def _from_dict(field_type, data):
    """Recursively convert a dictionary to a dataclass."""
    if isinstance(data, list):
        return [_from_dict(field_type.__args__[0], item) for item in data]
    elif isinstance(data, dict) and hasattr(field_type, '__dataclass_fields__'):
        fieldtypes = {}
        for f in field_type.__dataclass_fields__.values():
            fieldtypes[f.name] = f.type
            type_args = typing.get_args(f.type)
            if None in type_args and len(type_args) == 2:  # handle Optional[X] types -> cast to X
                fieldtypes[f.name] = type_args[0]
            else:
                fieldtypes[f.name] = f.type
        return field_type(**{f: _from_dict(fieldtypes[f], data[f]) for f in data})
    elif inspect.isclass(field_type) and issubclass(field_type, Enum):
        return field_type(data)
    else:
        return data


def _remove_null_entries(d):
    """Recursively remove keys with None values from a dictionary."""
    if isinstance(d, list):
        return [_remove_null_entries(item) for item in d if item is not None]
    if not isinstance(d, dict):
        return d
    return {
        k: _remove_null_entries(v)
        for k, v in d.items()
        if v is not None
    }


def _sha256_generate(localpath: pathlib.Path) -> str:
    if not localpath.exists():
        raise ValueError(f"File {localpath} does not exist for SHA256 generation")

    if shutil.which("sha256sum"): # calling subprocess will release the GIL when called in a thread
        result = subprocess.run(["sha256sum", str(localpath)], capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to generate SHA256 checksum: {result.stderr.strip()}")
        return result.stdout.split()[0]
    else:
        sha256_hash = hashlib.sha256()
        with open(localpath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()


Kind = typing.Literal[
    "switch_image",
    "nic_firmware",
    "binary",
]


@dataclass
class Artifact:
    kind: Kind
    description: str
    path: str  # http or file path to get the artifact
    name: Optional[str] = None  # Name of the artifact
    sha256: Optional[str] = None  # optional SHA256 checksum of the artifact
    properties: Optional[dict] = None  # optional properties of the artifact


@dataclass
class Metadata:
    name: str
    description: str
    release: str
    build: str


@dataclass
class Manifest:
    metadata: Metadata
    artifacts: List[Artifact]

    @classmethod
    def read_yaml(cls, file_path: str) -> 'Manifest':
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
            return _from_dict(Manifest, data)

    def write_yaml(self, file_path: typing.Union[str, typing.IO]):
        if isinstance(file_path, str):
            with open(file_path, "w") as f:
                yaml.dump(_remove_null_entries(asdict(self)), f, default_flow_style=False, sort_keys=False)
        else:
            yaml.dump(_remove_null_entries(asdict(self)), file_path, default_flow_style=False, sort_keys=False)


class Obj(abc.ABC):
    def size(self) -> int:
        """Return the size of the object in bytes."""
        raise NotImplementedError("Subclasses must implement this method.")

    def copy(self, dst_path: str):
        """Copy the object to the given destination path."""
        raise NotImplementedError("Subclasses must implement this method.")

    def sha256(self) -> str:
        """Return the SHA256 checksum of the object """
        raise NotImplementedError("Subclasses must implement this method.")


class HttpObj(Obj):
    """Object that can be accessed via HTTP."""

    def __init__(self, url: str):
        self.url = url

    def size(self) -> int:
        r = requests.head(self.url)
        if r.status_code == 404:
            return -1
        r.raise_for_status()
        return int(r.headers.get("Content-Length", 0))

    def copy(self, dst_path: str):
        logger.debug("Downloading %s to %s", self.url, dst_path)
        with requests.get(self.url, stream=True) as r:
            r.raise_for_status()
            with open(dst_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    def sha256(self) -> str:
        """Return the SHA256 checksum of the object by downloading it."""
        r = requests.get(self.url, stream=True)
        r.raise_for_status()
        sha256_hash = hashlib.sha256()
        for byte_block in r.iter_content(chunk_size=4096):
            sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def __str__(self) -> str:
        return self.url

class FileObj(Obj):
    """Object that is a local file."""

    def __init__(self, path: str):
        self.path = path

    def size(self) -> int:
        if not os.path.exists(self.path):
            return -1
        return os.path.getsize(self.path)

    def copy(self, dst_path: str):
        logger.debug("Copying file from %s to %s", self.path, dst_path)
        shutil.copyfile(self.path, dst_path)

    def sha256(self) -> str:
        """Return the SHA256 checksum of the local file."""
        return _sha256_generate(pathlib.Path(self.path))

    def __str__(self) -> str:
        return f"file://{self.path}"


class Context(object):
    """Context for the bundler, used to pass around common data."""

    def __init__(self, server: str):
        self.server = server

    def resolve(self, url: str) -> Obj:
        """Resolve the path to a full URL or local file path."""
        protocol, path = url.split("://", 1) if "://" in url else (None, url)
        if protocol is None:
            full_path = f"http://{self.server}/{path.lstrip('/')}"
            return HttpObj(full_path)
        elif protocol == "http" or protocol == "https":
            return HttpObj(url)
        elif protocol == "file":
            return FileObj(path)
        else:
            raise ValueError(f"Unsupported path format: {path}. Must be http://, https://, or file://")


def _sha256_compare(artifact: Artifact, localpath: pathlib.Path) -> Optional[bool]:
    """Check if the local file matches the SHA256 checksum of the artifact."""
    if not artifact.sha256:
        return None
    return _sha256_generate(localpath) == artifact.sha256


def verify(ctx: Context, manifest: Manifest, skip_sha: bool = False) -> Optional[Manifest]:
    """Check if the server contains all required release artifacts"""

    total_size = 0
    errors = []

    def _resolve_one(a: Artifact) -> int:
        """Resolve a single artifact to an object."""
        try:
            obj = ctx.resolve(a.path)
            s = obj.size()
            if s < 0:
                raise ValueError(f"{a.path} not found")
            if not skip_sha:
                if a.sha256:
                    logger.debug(f"Checking SHA256 for {a.path} ({s} bytes)")
                    if not a.sha256 == obj.sha256():
                        raise ValueError(f"sha256 mismatch")
                else:
                    logger.debug(f"Generating SHA256 for {a.path} ({s} bytes)")
                    a.sha256 = obj.sha256()
            a.path = str(obj)
            return s
        except Exception as e:
            raise ValueError(f"unexpected error: {e}")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_resolve_one, a): a for a in manifest.artifacts}
        for future in futures:
            artifact = futures[future]
            try:
                size = future.result()
                total_size += size
            except Exception as e:
                logger.error(f"Error resolving artifact {artifact.path}: {e}")
                errors.append(artifact.path)

    if errors:
        missing_str = "\n".join(errors)
        logger.error(f"Encountered {len(errors)} errors verifying artifacts: \n{missing_str}")
        return None

    logger.info(f"All {len(manifest.artifacts)} artifacts verified. "
                f"Total size: {total_size} bytes ({total_size / (1024 ** 3):.2f}GB)")
    return manifest


def _bundle_artifact(ctx: Context, artifact: Artifact, basedir: pathlib.Path) -> Artifact:
    """Download an artifact to the given directory. Return the downloaded path, or empty string if download failed"""
    file_dir = basedir / artifact.kind
    file_dir.mkdir(parents=True, exist_ok=True)
    file_name = os.path.basename(artifact.path)
    localpath = file_dir / file_name
    rv = Artifact(
        kind=artifact.kind,
        description=artifact.description,
        path=os.path.join(artifact.kind, file_name),
        name=artifact.name or file_name,
        sha256=artifact.sha256,
        properties=artifact.properties,
    )

    # check for cached file
    try:
        if _sha256_compare(artifact, localpath):
            logger.info(f"Artifact {artifact.path} already exists and matches SHA256 checksum.")
            return rv
    except ValueError as e:
        pass

    obj = ctx.resolve(artifact.path)
    obj.copy(str(localpath))
    # generate the sha256 checksum if not provided
    if not rv.sha256:
        rv.sha256 = _sha256_generate(localpath)
    return rv


def bundle(ctx: Context, manifest: Manifest, output: str) -> int:
    """ Bundle release artifacts into a tar file or a staging directory. """
    out_path = pathlib.Path(output)
    if out_path.suffix == ".tar":
        staging_dir = out_path.parent / out_path.name.rsplit(".", 1)[0]
        if out_path.exists():
            logger.warning(f"Unpacking existing tar file {output} temporarily to {staging_dir}")
            shutil.unpack_archive(output, staging_dir, "tar")
    else:
        staging_dir = out_path
    staging_dir.mkdir(exist_ok=True, parents=True)

    # collect artifacts
    start_time = time.time()
    logger.info(f"Collecting artifacts in {staging_dir}")
    artifacts = []
    errors = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_bundle_artifact, ctx, a, staging_dir): a for a in manifest.artifacts}
        for future in futures:
            a = futures[future]
            try:
                artifacts.append(future.result())
            except Exception as e:
                errors.append(f"Failed to copy {a.path}: {e}")
    if errors:
        error_str = "\n".join(errors)
        logger.error(f"Failed to copy {len(errors)} artifacts:\n{error_str}")
        return 1

    manifest.artifacts = artifacts
    manifest.write_yaml(os.path.join(staging_dir, "manifest.yaml"))

    # tar if requested
    if output.endswith(".tar"):
        valid_file = {a.path for a in manifest.artifacts}
        valid_dir = {a.kind for a in manifest.artifacts}

        def _filter(tarinfo):
            tarinfo.uid = 0
            tarinfo.gid = 0
            tarinfo.uname = "root"
            tarinfo.gname = "root"
            tarinfo.name = tarinfo.name[2:] if tarinfo.name.startswith("./") else tarinfo.name
            if tarinfo.isdir() and (tarinfo.name in valid_dir or tarinfo.name == "."):
                return tarinfo
            if tarinfo.name in valid_file or tarinfo.name == "manifest.yaml":
                return tarinfo
            return None

        logger.info(f"Creating bundle tar file: {output}")
        with tarfile.open(output, "w") as tar:
            tar.add(staging_dir, arcname=".", filter=_filter)
        shutil.rmtree(staging_dir)
    else:
        logger.info(f"Staged artifacts to: {staging_dir}")
    logger.info(f"Bundle staging completed in {(time.time() - start_time):.1f} seconds")
    return 0


def str2bool(v):
    return str(v).lower() in ('1', 'true', 'yes', 'on')


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate cluster deployment artifacts tar file"
    )
    parser.add_argument(
        "--server",
        default=FILE_SERVER,
        help=str("Artifacts host. Assumes server is an http server serving artifacts described in the path field of "
                 "the manifest. Default: " + FILE_SERVER),
    )
    parser.add_argument("--verbose", "-v", action="store_true", )

    def _common_args(p: argparse.ArgumentParser):
        p.add_argument(
            "MANIFEST",
            help="Path to the manifest file.",
        )
        p.add_argument("--release", "-r", help="Release version for metadata")
        p.add_argument("--build", "-b", help="Build version for metadata")

    subparsers = parser.add_subparsers(dest="command", help="subcommand")
    p = subparsers.add_parser("verify", help="Verify the availability and contents of given manifest")
    _common_args(p)
    p.add_argument("--skip-sha", 
                   action="store_true",
                   default=str2bool(os.environ.get("SKIP_SHA", "false")),
                   help="skip SHA256 verification of artifacts (faster)")

    p = subparsers.add_parser("bundle", help="bundle release artifacts")
    _common_args(p)
    p.add_argument("--output", required=True, help="path to output tar file or staging directory")

    if len(sys.argv) < 2:
        parser.print_help(sys.stderr)
        return 0

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", )
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    context = Context(server=args.server)
    try:
        manifest = Manifest.read_yaml(args.MANIFEST)
    except Exception as e:
        if args.verbose:
            logger.exception(f"unable to read manifest")
        else:
            logger.error(f"unable to read manifest: {e}")
        return 1

    manifest.metadata.release = args.release or "0.0.0"
    manifest.metadata.build = args.build or "deadbeef"

    if args.command == "verify":
        manifest = verify(context, manifest, skip_sha=args.skip_sha)
        if not manifest:
            return 1
        manifest.write_yaml(sys.stdout)
        return 0

    elif args.command == "bundle":
        return bundle(context, manifest, args.output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
