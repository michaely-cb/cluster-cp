import argparse
import json
import subprocess
import sys

# This script is running on each cluster node.
# cscfg python scripts and some python packages are not available on these nodes.


def get_expected(manifest: dict, info: dict) -> str:
    """Given the node info, return the expected value of a manifest"""
    if len(manifest["expected"]) == 0:
        return "N/A"

    # Return the first one that the scope covers
    for e in manifest["expected"]:
        s = e.get("scope")
        if s is None:
            return e["value"]

        if s["vendor"] != "" and s["vendor"] != info["vendor"]:
            continue
        if s["model"] != "" and s["model"] != info["model"]:
            continue
        if "role" in s and s["role"] != "" and s["role"] != info["role"]:
            continue
        # info matches scope
        return e["value"]

    # scope not matched
    return "Not specified"


def collect_node_status(name: str, vendor: str, role: str, manifest_file: str) -> int:
    """Core function to collect node status"""
    with open(manifest_file, "r") as file:
        manifest = json.load(file)

    # Since dataclasses package is not available, we just mirror the dataclass
    # with dictionaries.
    # ClusterStatus
    status = {
        "summary": {"nodes": {"num_nodes": 1}},
        "discrepancies": {"nodes": []},
        "details": {"nodes": []},
    }

    # ClusterNodeInfo
    node_info = {"vendor": vendor, "role": role}

    # ClusterNodeFeatures
    discrepancies = {"name": name, "info": node_info, "features": []}

    # ClusterNodeDetails
    details = {"name": name, "info": node_info, "features": []}

    errors = 0

    # for each NodeManifest:
    for m in manifest["node_manifest"]:
        feature_name = m["name"]
        expected = get_expected(m, node_info)
        if expected == "Not specified":
            # scope not matched
            continue

        if "cmd" not in m:
            print(
                f"Missing command to get feature {feature_name} on node {name}, skipped",
                file=sys.stderr,
            )
            errors += 1
            continue

        result = subprocess.run(m["cmd"], shell=True, stdout=subprocess.PIPE)
        out = result.stdout.decode("utf-8").strip()
        if result.stderr is not None:
            err = result.stderr.decode("utf-8").strip()
        else:
            err = ""

        if result.returncode:
            print(
                f"Error in getting feature {feature_name} on node {name}: stdout: {out}, stderr: {err}",
                file=sys.stderr,
            )
            errors += 1
            out = "Not found"

        feature = {"name": feature_name, "value": out}
        if expected != "N/A":
            feature["expected"] = expected

        details["features"].append(feature)
        if expected != "N/A" and feature["value"] != feature["expected"]:
            discrepancies["features"].append(feature)

    status["details"]["nodes"].append(details)
    if len(discrepancies["features"]) > 0:
        status["summary"]["nodes"]["num_discrepant_nodes"] = 1
        status["discrepancies"]["nodes"].append(discrepancies)

    print(json.dumps(status))

    return errors


def main() -> int:
    """Collect the cluster node status of this node and dump the ClusterStatus json to stdout"""

    parser = argparse.ArgumentParser(
        description="Collect the cluster node status of this node"
    )
    parser.add_argument("--node_name", required=True, help="name of this node")
    parser.add_argument("--vendor", required=True, help="node vendor")
    parser.add_argument("--role", required=True, help="node role")
    parser.add_argument(
        "--manifest", required=True, help="cluster_manifest.json input file"
    )

    args = parser.parse_args()

    return collect_node_status(args.node_name, args.vendor, args.role, args.manifest)


if __name__ == "__main__":
    sys.exit(main())
