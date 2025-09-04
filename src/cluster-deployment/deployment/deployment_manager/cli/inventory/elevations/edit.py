import tempfile
import logging
import yaml
from typing import List
from deployment_manager.cli.inventory.filter import Filter
from deployment_manager.cli.inventory.repr import Elevation
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.common.yamlhandler import CustomYamlLoader, CustomYamlDumper
from deployment_manager.tools.utils import edit_file
from deployment_manager.db.models import ElevationEntity
from django.db import transaction
import deployment_manager.cli.inventory.service as service


logger = logging.getLogger(__name__)

def do_editor_update(elevation_entities: List[ElevationEntity]) -> List[Elevation]:
    with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        elevations = [service.elevation_entity_to_repr(elevation_entity) for elevation_entity in elevation_entities]
        
        temp_file.write(yaml.dump(
            [elevation.dict() for elevation in elevations],
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
                    raise ValueError("YAML must contain a list of elevation objects")
                
                if docs == last_docs: ctr += 1
                else: ctr = 0
                last_docs = docs

                elevations = []

                validation_errors = []

                for idx, (doc, orig_entity) in enumerate(zip(docs, elevation_entities)):
                    if not doc or "name" not in doc or "device_specs" not in doc:
                        validation_errors.append(f"Elevation at index {idx} missing required fields: 'name' and/or 'device_specs'")
                        continue

                    if doc["name"] != orig_entity.name:
                        validation_errors.append(f"Changing elevation name is not permitted (index {idx})")
                        continue
                    
                    try:
                        elevation = Elevation(**doc)
                        validation_errors.extend(service.validate_elevation(elevation))
                        elevations.append(elevation)
                    except Exception as e:
                        validation_errors.append(str(e))
                
                if validation_errors:
                    elevations.clear()
                    raise ValueError("\n".join(validation_errors))

                return elevations

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
    """Edit an existing elevation"""
    name = "edit"

    def construct(self):
        self.add_arg_filter()
        self.add_arg_output_format()

    def run(self, args) -> int:
        fmt = args.output
        profile = service.get_profile(self.profile)

        query_set = service.get_query_set(profile, ElevationEntity)
        
        elevation_entities = Filter.filter_devices(
            ElevationEntity, args, query_set=query_set
        )

        if len(elevation_entities) == 0:
            logger.error(f"Elevations not found.")
            return 1

        elevations = do_editor_update(elevation_entities)

        if not elevations:
            logger.info("No changes applied.")
            return 0

        added_elevations = []
        try:
            with transaction.atomic():
                added_elevations = []
                for elevation in elevations:
                    service.update_elevation(elevation, profile)
                    added_elevations.append(elevation)
        except Exception as e:
            logger.error(str(e))
            return 1
        
        print(Elevation.format_reprs(added_elevations, fmt=fmt))
        return 0
