import pathlib
import sys
import os
import tarfile

# Ensure the bundler module can be imported from the parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from bundler import Manifest, Metadata, Artifact, Kind, Context, verify, bundle


def _create_test_artifacts(tmp_path: pathlib.Path):
    src_path = tmp_path / "src"
    src_path.mkdir()
    file1 = src_path / "file1.txt"
    file1.write_text("hello world")

    file2 = src_path / "file2.bin"
    file2.write_text("another file")

    manifest = Manifest(
        metadata=Metadata(name="foo", description="bar", release="test-release", build="test-build"),
        artifacts=[
            Artifact(
                kind="binary",
                description="Test file 1",
                path=f"file://{file1}",
                name="file1.txt"
            ),
            Artifact(
                kind="switch_image",
                description="Test file 2",
                path=f"file://{file2}",
                name="file2.bin",
                properties={"foo": "bar"},
            ),
        ]
    )
    return manifest


def test_verify(tmp_path: pathlib.Path):
    """
    Tests that the verify function correctly validates a manifest with existing local files.
    """
    manifest = _create_test_artifacts(tmp_path)
    ctx = Context(server="dummy-server")
    result = verify(ctx, manifest)

    assert result is not None
    assert len(result.artifacts) == 2

    # add a missing artifact to the result
    missing_artifact = Artifact(
        kind="binary",
        description="Missing file",
        path="file://non_existent.txt",
        name="non_existent.txt"
    )
    manifest.artifacts.append(missing_artifact)
    result = verify(ctx, manifest)
    assert result is None


def test_bundle_to_tar(tmp_path: pathlib.Path):
    """
    Tests that the bundle function correctly creates a tar file with the expected contents.
    """
    # 1. Create test artifacts and manifest
    manifest = _create_test_artifacts(tmp_path)
    ctx = Context(server="dummy-server")
    output_tar = tmp_path / "bundle.tar"

    # 2. Bundle the artifacts into a tar file
    result = bundle(ctx, manifest, str(output_tar))
    assert result == 0
    assert output_tar.exists()

    # 3. Untar the file and verify its contents
    untar_dir = tmp_path / "untarred"
    untar_dir.mkdir()
    with tarfile.open(output_tar, "r") as tar:
        tar.extractall(path=untar_dir)

    # Verify contents
    assert (untar_dir / "binary" / "file1.txt").read_text() == "hello world"
    assert (untar_dir / "switch_image" / "file2.bin").read_text() == "another file"

    manifest_path = untar_dir / "manifest.yaml"
    assert manifest_path.exists()
    bundled_manifest = Manifest.read_yaml(str(manifest_path))

    assert len(bundled_manifest.artifacts) == 2
    artifact1 = next(a for a in bundled_manifest.artifacts if a.name == "file1.txt")
    artifact2 = next(a for a in bundled_manifest.artifacts if a.name == "file2.bin")

    assert artifact1.path == "binary/file1.txt"
    assert artifact1.sha256 is not None
    assert artifact2.path == "switch_image/file2.bin"
    assert artifact2.sha256 is not None
    assert artifact2.properties == {"foo": "bar"}
