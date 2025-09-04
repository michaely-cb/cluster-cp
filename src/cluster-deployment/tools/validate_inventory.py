import csv
import argparse
import yaml
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, DefaultDict
from pydantic import BaseModel, ValidationError

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


class RackCounts(BaseModel):
    sr: Optional[Dict[str,int]]=None
    sw: Optional[Dict[str,int]]=None
    pu: Optional[Dict[str,int]]=None


class RackTemplate(BaseModel):
    name: str
    properties: Optional[Dict[str, str]] = None
    counts: Optional[RackCounts] = None


class Rack(BaseModel):
    template: str
    names: List[str]


class InventoryValidationSpec(BaseModel):
    kind: str
    rackTemplates: List[RackTemplate]
    racks: List[Rack]


def load_and_parse_inventory(file_path: str) -> DefaultDict[str, Dict[str, int]]:
    try:
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            required_columns = {"location.rack", "role", "type"}

            if not required_columns.issubset(reader.fieldnames):
                logging.error(f"CSV file is missing required columns: {required_columns - set(reader.fieldnames)}")
                exit(1)

            rack_counts = defaultdict(lambda: defaultdict(int))
            for row in reader:
                rack_name = row["location.rack"]
                role = row["role"].lower()
                type_ = row["type"].lower()
                rack_counts[rack_name][f"{type_}.{role}"] += 1

            return rack_counts
    except Exception as e:
        logging.error(f"Error loading CSV file '{file_path}': {e}")
        exit(1)


def load_and_parse_spec(file_path: str) -> InventoryValidationSpec:
    try:
        with open(file_path,"r") as file:
            yaml_data = yaml.safe_load(file)
        spec = InventoryValidationSpec(**yaml_data)
        return spec
    except ValidationError as e:
        logging.error(f"YAML validation error: {e}")
        exit(1)
    except Exception as e:
        logging.error(f"Error loading YAML file '{file_path}': {e}")
        exit(1)


def validate_inventory(rack_counts: DefaultDict[str, Dict[str, int]], spec: InventoryValidationSpec) -> List[str]:
    errors = []
    
    rack_to_template = {
        rack_name: rack.template
        for rack in spec.racks
        for rack_name in rack.names
    }

    for rack_name, counts in rack_counts.items():
        if rack_name not in rack_to_template:
            errors.append(f"Rack '{rack_name}' is found in the inventory but not defined in spec YAML.")
            continue

        template_name = rack_to_template[rack_name]
        template = next((t for t in spec.rackTemplates if t.name == template_name), None)
        if not template or not template.counts:
            continue

        expected_counts = template.counts.dict() 
        for type_, roles in expected_counts.items():
            if roles is None:
                continue
            for role, expected_count in roles.items():
                category = f"{type_}.{role}"
                actual_count = counts.get(category, 0)
                if actual_count != expected_count:
                    errors.append(
                        f"Rack '{rack_name}' ({template_name}): {category} count mismatch. "
                        f"Expected {expected_count}, found {actual_count}."
                    )

    missing_racks = set(rack_to_template.keys()) - set(rack_counts.keys())
    for missing_rack in missing_racks:
        errors.append(f"Rack '{missing_rack}' is defined in spec YAML but missing in the inventory file.")

    return errors


def main(spec_file: str, inventory_file: str) -> None:
    spec = load_and_parse_spec(spec_file)
    rack_counts = load_and_parse_inventory(inventory_file)

    errors = validate_inventory(rack_counts, spec)

    if errors:
        logging.error("Validation Failed:")
        for error in errors:
            logging.error(f" - {error}")
        exit(1)
    else:
        logging.info("Inventory validation passed successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate inventory CSV against YAML specification.")
    parser.add_argument("--template", required=True, help="Path to the YAML specification file")
    parser.add_argument("--inventory",required=True, help="Path to the inventory CSV file")
    args = parser.parse_args()

    main(args.template, args.inventory)
