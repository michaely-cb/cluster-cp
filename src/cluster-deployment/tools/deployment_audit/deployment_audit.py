from collections import defaultdict
from tabulate import tabulate
from pathlib import Path
import subprocess
import yaml
import sys
import os

def run():
    if len(sys.argv) < 3:
        print("Usage: python3 deployment_audit.py <template_file> <racks_file>")
        print("Error: This program expects two inputs: a template file and a racks file.")
        print("Example: python3 deployment_audit.py msp1_template.yaml cg6.yaml")
        sys.exit(1)

    template_file = sys.argv[1]
    racks_file = sys.argv[2]

    template = open_file(template_file)
    racks = open_file(racks_file)

    return template, racks

def execute_command(command: str):
    """Execute a shell command and return the output as YAML."""
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return yaml.safe_load(result.stdout)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Command failed with error:\n{e.stderr}") from e
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse output as YAML:\n{result.stdout}") from e

def open_file(path: str):
    """Open a YAML file from {path} and return its contents."""
    filepath = Path(path)
    try:
        with filepath.open("r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {filepath}")
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse YAML in file: {filepath}") from e

def init_data(is_prod_environment=False):
    global cscfg_systems, cscfg_servers, cscfg_all_devices, cscfg_spine, k8_systems, k8_servers, stamp, cg3_systems
    if is_prod_environment:
        if stamp == "dh1":
            cscfg_systems = execute_command(f"sudo cscfg deploy show -f type=SY -o yaml")
            cscfg_servers = execute_command(f"sudo cscfg deploy show -f type=SR -o yaml")
            cscfg_all_devices = execute_command(f"sudo cscfg deploy show -o yaml")
            cscfg_spine = execute_command("sudo cscfg deploy show -f role=SP -o yaml")
            k8_systems = execute_command("sudo kubectl get systems -o yaml")
            k8_servers = execute_command("sudo kubectl get nodes -o yaml")
        elif stamp == "cg3":
            cscfg_systems = execute_command(f"sudo cscfg deploy show -f type=SY location.stamp={stamp} -o yaml")
            cscfg_servers = execute_command(f"sudo cscfg deploy show -f type=SR location.stamp={stamp} -o yaml")
            cscfg_all_devices = execute_command(f"sudo cscfg deploy show -f location.stamp={stamp} -o yaml")
            cscfg_spine = execute_command("sudo cscfg deploy show -f role=SP -o yaml")
            k8_systems = execute_command("sudo kubectl get systems -o yaml")
            k8_servers = execute_command("sudo kubectl get nodes -o yaml")
            cg3_systems = execute_command(f"sudo cscfg device show -f type=SY location.stamp=cg3 -o yaml")
            create_cg3_mapping()
        else:
            cscfg_systems = execute_command(f"sudo cscfg deploy show -f type=SY location.stamp={stamp} -o yaml")
            cscfg_servers = execute_command(f"sudo cscfg deploy show -f type=SR location.stamp={stamp} -o yaml")
            cscfg_all_devices = execute_command(f"sudo cscfg deploy show -f location.stamp={stamp} -o yaml")
            cscfg_spine = execute_command("sudo cscfg deploy show -f role=SP -o yaml")
            k8_systems = execute_command("sudo kubectl get systems -o yaml")
            k8_servers = execute_command("sudo kubectl get nodes -o yaml")
    else:
        cscfg_systems = open_file(f"inventory/{stamp}/cscfg_systems.yaml")
        cscfg_servers = open_file(f"inventory/{stamp}/cscfg_servers.yaml")
        cscfg_all_devices = open_file(f"inventory/{stamp}/cscfg_all_devices.yaml")
        cscfg_spine = open_file(f"inventory/{stamp}/cscfg_spine.yaml")
        k8_systems = open_file(f"inventory/{stamp}/k8_systems.yaml")
        k8_servers = open_file(f"inventory/{stamp}/k8_servers.yaml")
        if stamp == "cg3":
            cg3_systems = open_file(f"inventory/{stamp}/cg3_systems.yaml")
            create_cg3_mapping()
    
    included_racks = [name for _racks in racks["racks"] for name in _racks["names"]]

    cscfg_systems = [
        device for device in cscfg_systems
        if any(device["name"].startswith(prefix) for prefix in included_racks)
    ]

    cscfg_servers = [
        device for device in cscfg_servers
        if any(device["name"].startswith(prefix) for prefix in included_racks)
    ]

    cscfg_all_devices = [
        device for device in cscfg_all_devices
        if any(device["name"].startswith(prefix) for prefix in included_racks)
    ]

    cscfg_spine = [
        device for device in cscfg_spine
        if any(device["name"].startswith(prefix) for prefix in included_racks)
    ]

    k8_systems["items"] = [
        device for device in k8_systems["items"]
        if any(device["metadata"]["name"].startswith(prefix) for prefix in included_racks)
    ]

    k8_servers["items"] = [
        device for device in k8_servers["items"]
        if any(device["metadata"]["name"].startswith(prefix) for prefix in included_racks)
    ]

def create_cg3_mapping():
    global cg3_systems, rack_to_xs_map
    rack_to_xs_map = defaultdict(list)
    for system in cg3_systems:
        rack_to_xs_map[system["properties"]["location"]["rack"].lower()].append(system["name"])
        

cscfg_systems = cscfg_servers = cscfg_all_devices = cscfg_spine = k8_systems = k8_servers = cg3_systems = rack_to_xs_map = None

template, racks = run()
stamp = racks.get("name", "")
print(f"Stamp: {stamp}")

init_data(is_prod_environment=True)

# ______________________PB Status______________________

def get_cscfg_names(data):
    return {
        entry.get("name", "")
        for entry in data
        if entry.get("name")
    }

def get_kubectl_names(data):
    return {
        item.get("metadata", {}).get("name", "")
        for item in data.get("items", [])
        if item.get("metadata", {}).get("name")
    }

def make_pb_table(table_type):
    kubectl_names = get_kubectl_names(k8_systems if table_type=="system" else k8_servers)
    cscfg_names = get_cscfg_names(cscfg_systems if table_type=="system" else cscfg_servers)

    table = []
    for name in sorted(cscfg_names):
        if name not in kubectl_names:
            table.append([
                name,
                "✅",
                ""
            ])
    return table

systems_table = make_pb_table("system")
servers_table = make_pb_table("server")

print("\nSystems (sy):")
print(tabulate(systems_table, headers=["Device", "pb2", "k8s"], tablefmt="grid"))
print("\nServers (sr):")
print(tabulate(servers_table, headers=["Device", "pb2", "k8s"], tablefmt="grid"))

# ______________________Inventory Audit______________________

rack_templates = {rt["name"]: rt["counts"] for rt in template["rackTemplates"]}
racks = racks["racks"]

expected_device_names = set()
expected_device_counts = defaultdict(int)  # dict with key (type, role) -> count

for rack in racks:
    template_name = rack["template"]
    rack_names = rack["names"]
    counts = rack_templates.get(template_name, {})

    for rack_name in rack_names:
        for role, type_counts in counts.items():
            if not isinstance(type_counts, dict):
                continue

            for dev_type, num in type_counts.items():
                alias_type = "mx" if dev_type in ["ml", "mxxl"] else dev_type
                expected_device_counts[(role, alias_type)] += int(num)

                for i in range(1, int(num) + 1):
                    index = f"{i:02}"

                    if role == "pu":
                        expected_device = f"{rack_name}-pdu{index}"
                    
                    elif role == "pr":
                        expected_device = f"{rack_name}-{dev_type}{index}"
                    
                    elif role == "cn":
                        expected_device = f"{rack_name}-cn{index}"
                    
                    elif role == "sy":
                        if stamp == "cg3":
                            expected_device = f"{rack_to_xs_map[rack_name][0]}"
                        elif stamp == "dh1":
                            expected_device = f"dh1-{rack_name}-{dev_type}-{role}{index}"
                        else:
                            expected_device = f"{rack_name}-{dev_type}-{role}{index}"
                                        
                    else:
                        expected_device = f"{rack_name}-{dev_type}-{role}{index}"

                    if rack_name == "inf000":
                        expected_device += "-virtual"

                    expected_device_names.add(expected_device)

actual_device_names = set()
actual_device_stage = defaultdict(str)  # key is (device name) -> device stage
actual_device_status = defaultdict(str)  # key is (device name) -> device status
actual_device_counts = defaultdict(int)  # dict with key (type, role) -> count

for device in (cscfg_all_devices + cscfg_spine):
    name = device.get("name")
    if not name or name in actual_device_names: continue
    
    actual_device_names.add(name)
    actual_device_stage[name] = device.get("stage", "")
    actual_device_status[name] = device.get("status", "")
    actual_device_counts[(device.get("type", "").lower(), device.get("role", "").lower())] += 1

all_keys = sorted(set(expected_device_counts) | set(actual_device_counts))
table = []
for role, dev_type in all_keys:
    expected = expected_device_counts[(role, dev_type)]
    actual = actual_device_counts[(role, dev_type)]
    table.append([dev_type, role, expected, actual, expected - actual])

headers = ["Role", "Type", "Expected", "Actual", "Diff"]
print("\n", tabulate(table, headers=headers, tablefmt="simple"), "\n")

only_expected = expected_device_names - actual_device_names
only_actual = actual_device_names - expected_device_names
common = expected_device_names & actual_device_names

table = []
missing_count = 0
new_count = 0
failed_count = 0

for name in sorted(only_expected):
    table.append([name, "Missing", "", ""])
    missing_count += 1

for name in sorted(only_actual):
    stage = actual_device_stage[name]
    status = actual_device_status[name]
    table.append([name, "Not in Spec", stage, status])
    new_count += 1

for name in sorted(common):
    status = actual_device_status[name]
    if status == "FAILED":
        stage = actual_device_stage[name]
        table.append([name, "Yes", stage, status])
        failed_count += 1

headers = ["Device", "Exists", "Stage", "Status"]
print(tabulate(table, headers=headers, tablefmt="grid"), "\n")

legend = [
    ["Missing devices", missing_count],
    ["New (not in spec)", new_count],
    ["Failed devices", failed_count],
    ["Total issues", missing_count + new_count + failed_count],
]
print("Summary:")
print(tabulate(legend, tablefmt="simple"), "\n")