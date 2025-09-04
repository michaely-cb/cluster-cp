"""
Launch the DB tests in containers. Automatically runs each test file in tests_container/

The container env will have django + other required deps. If you have a venv
with django, you can run `make unittest` for the same effect with easier local
debugging
"""
import pathlib

import pytest

from deployment_manager.tests.conftest import MockCluster


@pytest.fixture(scope="module")
def dbtest_cluster():
    with MockCluster("dbtest_django") as cluster:
        cluster.must_exec_root("/project/install.sh")
        # below files are needed for switch ZTP test to work
        cluster.must_exec_root("mkdir -p /var/www/html")
        cluster.must_exec_root("touch /var/www/html/EOS64-4.33.1F.swi")
        cluster.must_exec_root("touch /var/www/html/EOS64-4.32.0F.swi")
        yield cluster


def _run_test(c: MockCluster, test: str):
    rv, out, err = c.exec(
        "rootserver",
        str("bash -c 'PYTHONPATH=/project/deployment GITTOP=/project "
            f"python deployment_manager/manage_db.py test {test}'"),
        workdir="/project/deployment"
    )
    assert rv == 0, f"{test} failed: {out}\n{err}"


def pytest_generate_tests(metafunc):
    if "test_module" in metafunc.fixturenames:
        test_modules = [
            f.name[:-3]
            for f in (pathlib.Path(__file__).parent.parent / "tests_container").iterdir()
            if f.name.endswith(".py")
        ]
        metafunc.parametrize("test_module", test_modules)


def test_containerized_unittest(dbtest_cluster, test_module):
    _run_test(dbtest_cluster, f"deployment_manager.tests_container.{test_module}")
