#!/usr/bin/env python
import sys
from db.utils import init_orm

if __name__ == "__main__":
    from django.core.management import execute_from_command_line

    init_orm()
    execute_from_command_line()

    if "migrate" in sys.argv:
        # SW-119860.  Temporary code for migrating inventory.csv to the database
        # The below code can be removed once all cluster deployment manager has been
        # updated to atleast rel-2.2.2
        import os
        from deployment_manager.db.models import DeploymentProfile
        from deployment_manager.tools.config import ConfGen
        profiles = DeploymentProfile.objects.all()
        if profiles:
            for profile in profiles:
                cg = ConfGen(profile.name)
                if not os.path.exists(cg.inventory_file):
                    continue
                with open(cg.inventory_file, "r") as fp:
                    line = fp.readline()
                if "# Do not edit" in line:
                    continue
                # Update the database and generate the inventory file
                try:
                    print(f"  Importing inventory data for profile {profile.name}")
                    cg.backup_inventory_file()
                    cg.update_inventory_from_file()
                    cg.generate_inventory_csv()
                except Exception as exc:
                    print(exc)
                    print("This issue maybe resolved by re-creating the profile " \
                          "or rename and fix the inventory file: ", cg.tmp_inventory_file)
            print("")
