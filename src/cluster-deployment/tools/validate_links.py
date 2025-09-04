import csv
import re
import yaml
import argparse
import logging
from collections import defaultdict
from pydantic import BaseModel, ValidationError
from typing import List, Dict, Optional


class LinkMatcher(BaseModel):
    src: Optional[str] = None
    dst: Optional[str] = None
    src_if: Optional[str] = None
    dst_if: Optional[str] = None
    count: int


class NodeTemplate(BaseModel):
    name: str
    srcLinkMatchers: Optional[List[LinkMatcher]] = []
    dstLinkMatchers: Optional[List[LinkMatcher]] = []


class Node(BaseModel):
    template: str
    names: List[str]


class ValidationSpec(BaseModel):
    kind: str
    nodeTemplates: List[NodeTemplate]
    nodes: List[Node]


def load_validation_rules(file_path: str) -> ValidationSpec:
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
        return ValidationSpec(**data)


def load_links(file_path: str) -> List[Dict[str, str]]:
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        return list(reader)


def expand_templates(validation_spec: ValidationSpec) -> List[Dict]:
    expanded_rules = []
    template_map = {template.name: template for template in validation_spec.nodeTemplates}

    for node in validation_spec.nodes:
        template = template_map[node.template]
        for name in node.names:
            for matcher in getattr(template, "srcLinkMatchers", []):
                rule = {
                    "name": f"{template.name}-{name}-src-{matcher.dst}-dst-{matcher.dst_if}",
                    "src": name,
                    "dst": matcher.dst,
                    "src_if": matcher.src_if,
                    "dst_if": matcher.dst_if,
                    "count": matcher.count,
                }
                expanded_rules.append(rule)

            for matcher in getattr(template, "dstLinkMatchers", []):
                rule = {
                    "name": f"{template.name}-{name}-dst-{matcher.src}-src-{matcher.src_if}",
                    "src": matcher.src,
                    "dst": name,
                    "dst_if": matcher.dst_if,
                    "count": matcher.count,
                }
                expanded_rules.append(rule)

    return expanded_rules


def validate_links(links: List[Dict[str, str]], expanded_rules: List[Dict]):
    results = defaultdict(int)

    for link in links:
        for rule in expanded_rules:
            if (
                (not rule["src"] or re.match(rule["src"], link["src_name"]))
                and (not rule["dst"] or re.match(rule["dst"], link["dst_name"]))
                and (not rule.get("src_if") or re.match(rule["src_if"], link["src_if"]))
                and (not rule["dst_if"] or re.match(rule["dst_if"], link["dst_if"]))
            ):
                results[rule["name"]] += 1

    for rule in expanded_rules:
        rule_name = rule["name"]
        src = rule["src"]
        dst = rule["dst"]
        expected_count = rule["count"]
        actual_count = results[rule_name]
        if actual_count != expected_count:
            logger.error(f"[FAILED] Rule: '{rule_name}' | SRC: '{src}' | DST: '{dst}' | Expected: {expected_count}, Got: {actual_count}")
        else:
            logger.info(f"[PASSED] Rule: '{rule_name}' | SRC: '{src}' | DST: '{dst}' | Expected: {expected_count}, Got: {actual_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate links against rules.")
    parser.add_argument("--template", help="Path to the YAML file containing validation rules.", required=True)
    parser.add_argument("--inventory", help="Path to the CSV file containing links.", required=True)
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose (debug-level) logging.")
    parser.add_argument("--error-only", "-e", action="store_true", help="Only show error messages.")
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.ERROR if args.error_only else logging.INFO
    logging.basicConfig(level=log_level, format="%(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    error_occurred = False 

    try:
        validation_spec = load_validation_rules(args.template)
        links = load_links(args.inventory)

        expanded_rules = expand_templates(validation_spec)
        validate_links(links, expanded_rules)

    except ValidationError as e:
        logger.error(f"Validation Error: {e}")
        error_occurred = True
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        error_occurred = True

    if error_occurred:
        exit(1)
