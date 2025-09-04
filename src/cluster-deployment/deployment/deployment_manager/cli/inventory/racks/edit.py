import tempfile
import logging
import yaml

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.cli.inventory.repr import Rack
from deployment_manager.common.yamlhandler import CustomYamlLoader, CustomYamlDumper
from deployment_manager.tools.utils import edit_file
from deployment_manager.db.models import RackEntity
from deployment_manager.cli.inventory.filter import Filter
from django.db import transaction
from typing import List, Tuple
import deployment_manager.cli.inventory.service as service


logger = logging.getLogger(__name__)


def do_editor_update(rack_entities: List[RackEntity], profile: str) -> List[Rack]:
    with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        racks = [service.rack_entity_to_repr(rack_entity) for rack_entity in rack_entities]
        temp_file.write(yaml.dump(
            [rack.dict() for rack in racks],
            Dumper=CustomYamlDumper,
            default_flow_style=False
        ).encode())
        temp_file.flush()

        last_docs = None
        ctr = 0

        while True:
            try:
                edit_file(temp_file.name)
                temp_file.seek(0)
                docs = yaml.load(temp_file, Loader=CustomYamlLoader)

                if not isinstance(docs, list) or not docs:
                    raise ValueError("YAML must contain a list of rack objects")
                
                if docs == last_docs: ctr += 1
                else: ctr = 0
                last_docs = docs

                racks = []

                validation_errors = []

                for idx, (doc, orig_entity) in enumerate(zip(docs, rack_entities)):
                    if not doc or "name" not in doc or "elevation_name" not in doc or "part_number" not in doc:
                        validation_errors.append(f"Rack at index {idx} missing required fields: 'name', 'elevation_name', or 'part_number'")
                        continue

                    if doc["name"] != orig_entity.name:
                        validation_errors.append(f"Changing rack name is not permitted (index {idx})")
                        continue
                    
                    try:
                        rack = Rack(**doc)
                        validation_errors.extend(service.validate_rack(rack, profile))
                        racks.append(rack)
                    except Exception as e:
                        validation_errors.append(str(e))
                    
                if validation_errors:
                    racks.clear()
                    raise ValueError("\n".join(validation_errors))
                
                return racks

            except Exception as e:
                logger.error("%s", e)

                if ctr >= 2:
                    logger.warning("User exited editor twice without making edits. Aborting update.")
                    return []
                
                with open(temp_file.name, "r") as f:
                    content = f.read()

                error_comments = "# ERROR:\n"
                for error_line in str(e).splitlines():
                    error_comments += f"# {error_line}\n"
                error_comments += "# Please fix and save again.\n\n"

                # Strip old error comments (leading/trailing # lines)
                content = "\n".join(
                    [line for line in content.splitlines() if not line.lstrip().startswith("#")]
                )

                with open(temp_file.name, "w") as f:
                    f.write(error_comments + content)
                continue

class Edit(SubCommandABC):
    """Edit an existing rack"""
    name = "edit"

    def construct(self):
        self.add_arg_filter()
        self.add_arg_output_format()

    def run(self, args) -> int:
        fmt = args.output
        profile = service.get_profile(self.profile)

        query_set = service.get_query_set(profile, RackEntity)
        
        rack_entities = Filter.filter_devices(
            RackEntity, args, query_set=query_set
        )

        if len(rack_entities) == 0:
            logger.error(f"Racks not found.")
            return 1

        racks = do_editor_update(rack_entities, profile)

        if not racks:
            logger.info("No changes applied.")
            return 0

        added_racks = []
        try:
            with transaction.atomic():
                added_racks = []
                for rack in racks:
                    service.update_rack(rack, profile)
                    added_racks.append(rack)
        except Exception as e:
            logger.error(str(e))
            return 1
        
        print(Rack.format_reprs(added_racks, fmt=fmt))
        return 0
