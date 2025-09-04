import pytest

from django.core.management import call_command
from django.test import TestCase


@pytest.mark.skip
class DbTest(TestCase):
    def test_for_missing_migrations(self):
        """
        Checks for pending model changes that have not been captured in a migration file.
        """
        try:
            # The --check and --dry-run flags tell makemigrations to exit with a
            # non-zero status if it detects changes, without writing any files.
            call_command("makemigrations", "--check", "--dry-run")
        except SystemExit:
            self.fail(
                "Pending model changes were found. Please run 'make -C $GITTOP/src/cluster_deployment/deployment migrations'"
                " and check in the generated migration files."
            )
