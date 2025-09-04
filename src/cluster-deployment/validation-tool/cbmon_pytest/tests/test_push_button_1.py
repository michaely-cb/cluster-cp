import os
import pytest
import subprocess
import tempfile
import re
from common_init import current_dir, ClusterInfoObj

#####################################################
# Global variables
#####################################################
REFERENCE_DATA_DIR = os.path.join(current_dir, "push_button_1", "REFERENCE_DATA")
OVERRIDE_REFERENCE_DATA_DIR = os.path.join(current_dir, "push_button_1", "OVERRIDE_REFERENCE_DATA")

#####################################################
# Common Code
#####################################################
def run_diff(file1, file2):
    try:
        # Run the diff command and capture the output
        process = subprocess.Popen(['diff', file1, file2], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        # Get the output and error streams
        stdout, stderr = process.communicate()

        # Check the return code to determine if the diff was successful
        if process.returncode == 0:
            # Print the diff output
            print(stdout)
        else:
            # Print an error message if the diff failed
            print("Diff failed with error:")
            print(stdout)
            print(stderr)

    except FileNotFoundError:
        print("Diff command not found. Make sure 'diff' is installed.")

def compare_output_regoldenize_if_needed(output_data, ref_file_name, ssh_host='', host_type=None, host_tag=None, only_check_overrides=False, nodeGroup=None):
    if nodeGroup is not None:
        ref_file_name = f"nodeGroup_{nodeGroup}/{ref_file_name}"
        
    reference_file = None
    reference_data = None
    override_reference_dir = os.path.join(OVERRIDE_REFERENCE_DATA_DIR, ClusterInfoObj.get_system_name(), ssh_host)
    override_reference_file = os.path.join(override_reference_dir, ref_file_name)
    override_reference_data = None

    print(f"Output:\n{output_data}")

    if host_tag is not None:
        if host_type is not None:
            if os.path.exists(os.path.join(REFERENCE_DATA_DIR, f"{host_tag}-{host_type}", ref_file_name)):
                reference_file = os.path.join(REFERENCE_DATA_DIR, f"{host_tag}-{host_type}", ref_file_name)
    if reference_file is None and host_type is not None:
        if os.path.exists(os.path.join(REFERENCE_DATA_DIR, host_type, ref_file_name)):
            reference_file = os.path.join(REFERENCE_DATA_DIR, host_type, ref_file_name)

    if reference_file is None:
        #if os.path.exists(os.path.join(REFERENCE_DATA_DIR, ref_file_name)):
        reference_file = os.path.join(REFERENCE_DATA_DIR, ref_file_name)

    ###### Code for creating reference-logs in central area ######

    # TODO: See if the following condition makes sense now that we have a "show_diff" make target.
    # Assumption is that this should be supported when cbmon_pytest is not installed in /opt (and is part of Github)
    # current_file_path = os.path.abspath(__file__)
    # if not current_file_path.startswith('/opt'):
    if True:
        create_reference = False
        if 'CREATE_REFERENCE_LOGS' in os.environ and os.environ['CREATE_REFERENCE_LOGS'] == "1":
            # TODO: Put more checks for this, to only allow this to happen centrally
            if not os.path.exists(reference_file):
                create_reference = True

        if 'CREATE_HOST_TYPE_REFERENCE_LOGS' in os.environ and os.environ['CREATE_HOST_TYPE_REFERENCE_LOGS'] == "1":
            os.makedirs(os.path.join(REFERENCE_DATA_DIR, host_type), exist_ok=True)
            reference_file = os.path.join(REFERENCE_DATA_DIR, host_type, ref_file_name)
            create_reference = True

        if 'CREATE_HOST_TAG_TYPE_REFERENCE_LOGS' in os.environ and os.environ['CREATE_HOST_TAG_TYPE_REFERENCE_LOGS'] == "1":
            os.makedirs(os.path.join(REFERENCE_DATA_DIR, f"{host_tag}-{host_type}"), exist_ok=True)
            reference_file = os.path.join(REFERENCE_DATA_DIR, f"{host_tag}-{host_type}", ref_file_name)
            create_reference = True

        if create_reference:
            with open(reference_file, "w") as file:
                file.write(output_data)

    ##############################################################

    if not only_check_overrides:
        with open(reference_file, "r") as file:
            reference_data = file.read()

        if output_data == reference_data:
            print(f"Output matches the reference-data in {reference_file}")
            return 0
    else:
        if not os.path.exists(override_reference_file):
            os.makedirs(override_reference_dir, exist_ok=True)
            with open(override_reference_file, "w") as file:
                file.write(output_data)

    if 'REGOLDENIZE_TESTS' in os.environ:
        os.makedirs(override_reference_dir, exist_ok=True)
        with open(override_reference_file, "w") as file:
            file.write(output_data)

    if os.path.exists(override_reference_file):
        with open(override_reference_file, "r") as file:
            override_reference_data = file.read()

        if output_data == override_reference_data:
            if not only_check_overrides:
                pytest.xfail(f"Mismatch with golden, but passing with override data.\nThe output can be viewed as part of the 'Captured stdout call' section")
                return 0
            else:
                return 0

    # Create a temporary file
    _, temp_file_name = tempfile.mkstemp()

    try:
        with open(temp_file_name, 'w') as temp_file:
            temp_file.write(output_data)

        if os.path.exists(reference_file):
            print(f"Difference between the 'current' output and 'reference' ({reference_file}) data is:")
            run_diff(temp_file_name, reference_file)

        if os.path.exists(override_reference_file):
            print(f"Difference between the 'current' output and 'override-reference' ({override_reference_file}) data is:")
            run_diff(temp_file_name, override_reference_file)

    finally:
    # Delete the temporary file
        os.remove(temp_file_name)

    return 1

#####################################################
# Tests Start from here
#####################################################

def test_cluster_info_summary(ref_file_name):
    result = ClusterInfoObj.get_cluster_info_summary_as_table()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, only_check_overrides=True)
    assert status == 0, "Cluster's Host count(s) seem to have changed"

def test_cluster_info_detailed(ref_file_name):
    result = ClusterInfoObj.get_cluster_info_detailed_as_table()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, only_check_overrides=True)
    assert status == 0, "Cluster's Host configuration(s) seems to have changed"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_kubernetes_version(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('kubelet --version 2>&1')
    result = stdout.read().decode()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
    assert status == 0, "Wrong Kubernetes Version"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_os_version(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('cat /etc/os-release 2>&1')
    result = stdout.read().decode()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
    assert status == 0, "Wrong OS Installation"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_kernel_version(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('uname -r 2>&1')
    result = stdout.read().decode()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
    assert status == 0, "Kernel version mismatch"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_number_of_cpu(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('nproc 2>&1')
    result = stdout.read().decode()
    result = f"Number of CPU: {result}"

    nodeGroup = None
    if host_type in ["memx_server"]:
        nodeGroup = ClusterInfoObj._all_servers[ssh_host]['nodeGroup']

    status = compare_output_regoldenize_if_needed(output_data=result, 
                                                  ref_file_name=ref_file_name, 
                                                  ssh_host=ssh_host, 
                                                  host_type=host_type, 
                                                  host_tag=host_tag,
                                                  nodeGroup=nodeGroup)
    assert status == 0, "Mismatch in CPU count"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_ram_and_swap(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('free -g | awk \'{print $1, $2}\' | tail -2 2>&1')
    result = stdout.read().decode()

    nodeGroup = None
    if host_type in ["memx_server"]:
        nodeGroup = ClusterInfoObj._all_servers[ssh_host]['nodeGroup']

    status = compare_output_regoldenize_if_needed(output_data=result, 
                                                  ref_file_name=ref_file_name, 
                                                  ssh_host=ssh_host, 
                                                  host_type=host_type, 
                                                  host_tag=host_tag,
                                                  nodeGroup=nodeGroup)
    assert status == 0, "Wrong Memory/Swap Configured"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_root_partition_size(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('df -h / --output=size 2>&1')
    result = stdout.read().decode()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
    assert status == 0, "Incorrect Root-partition Configured"

'''
# Disabling this test for now, since mount is hanging on some machines, and not sure if the test is needed.
@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_mount_cb_check(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('df -h /cb --output=source 2>&1')
    result = stdout.read().decode()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
    assert status == 0, "Issue with '/cb' mount"
'''

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_interface_driver_details(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('i=(`nmcli -t -f DEVICE conn show --active | tail -n 1`); ethtool -i $i | grep -w "^driver\\|^version" 2>&1')
    result = stdout.read().decode()

    nodeGroup = None
    if host_type in ["memx_server"]:
        nodeGroup = ClusterInfoObj._all_servers[ssh_host]['nodeGroup']

    status = compare_output_regoldenize_if_needed(output_data=result, 
                                                  ref_file_name=ref_file_name, 
                                                  ssh_host=ssh_host, 
                                                  host_type=host_type, 
                                                  host_tag=host_tag,
                                                  nodeGroup=nodeGroup)
    assert status == 0, "Wrong Interface Driver Configuration"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_raid_configuration(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('[ ! -f /proc/mdstat ] || mdadm --detail /dev/md127 | grep Active 2>&1')
    result = stdout.read().decode()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
    assert status == 0, "Incorrect RAID Configuration"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_chassis_manufacturer(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('dmidecode -s chassis-manufacturer 2>&1')
    result = stdout.read().decode()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
    assert status == 0, "Chassis Manufacturer details mismatch"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_processor_version(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('dmidecode -s processor-version | grep -v "Not Specified" | cut -d " " -f 3 2>&1')
    result = stdout.read().decode()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
    assert status == 0, "Processor Version details mismatch"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_processor_frequency(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('dmidecode -s processor-frequency | grep -v "Unknown" 2>&1')
    result = stdout.read().decode()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
    assert status == 0, "Processor Frequency details mismatch"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_hyperthreading_configuration(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('cat /sys/devices/system/cpu/smt/active 2>&1')
    result = stdout.read().decode()
    
    nodeGroup = None
    if host_type in ["memx_server"]:
        nodeGroup = ClusterInfoObj._all_servers[ssh_host]['nodeGroup']
    
    status = compare_output_regoldenize_if_needed(output_data=result, 
                                                    ref_file_name=ref_file_name, 
                                                    ssh_host=ssh_host, 
                                                    host_type=host_type, 
                                                    host_tag=host_tag,
                                                    nodeGroup=nodeGroup)
    assert status == 0, "Incorrect Configuration of Hyper-Threading"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_tcp_liberal_flag_configuration(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('cat /proc/sys/net/netfilter/nf_conntrack_tcp_be_liberal 2>&1')
    result = stdout.read().decode()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
    assert status == 0, "Incorrect Configuration of TCP Liberal Flag"

@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_network_hardware_configuration(client, ssh_host, host_type, host_tag, ref_file_name):
    _, stdout, _ = client.exec_command('lshw -short -C network | grep -v ^==== | awk \'{ printf "%-15s %-15s %-15s %-15s", $1, $2, $3, $4; for (i=5; i<=NF; i++) printf " %s", $i; printf "\\n"}\' 2>&1')
    result = stdout.read().decode()
    status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
    assert status == 0, "Incorrect Network Hardware Configuration"

################################################################################
# RoCE validations
@pytest.mark.parametrize("ssh_host", ClusterInfoObj.get_memx_servers() + ClusterInfoObj.get_swarmx_servers())
def test_pcf_enabled(client, ssh_host):
    host_ports = ClusterInfoObj._all_servers[ssh_host]['ports']
    if not host_ports:
        pytest.skip(f"No ports defined")
    
    executed = False
    foundPort = False
    for port in host_ports:
        if not port["name"].startswith("P"):
            continue
        foundPort = True
        if "nic" not in port:
            print(f"Skipping {ssh_host} port {port['name']}, no nic defined")
            continue
        executed = True
        _, stdout, _ = client.exec_command(f'mlnx_qos -i {port["nic"]} 2>&1')
        result = stdout.read().decode()
        assert "Priority trust state: dscp" in result, f"{port['name']}: Priority trust state is not dscp"
        
        rs = [l.strip() for l in result.split("\n")]
        pfc_index = rs.index("PFC configuration:")
        assert re.match(r'enabled\s+(\d\s+){5}1', rs[pfc_index + 2]) is not None,  f"{port['name']}: Priority 5 is not enabled"
        assert re.match(r'buffer\s+(\d\s+){5}1', rs[pfc_index + 3]) is not None,  f"{port['name']}: Priority 5 buffer is not enabled"
    if not foundPort:
        pytest.skip("Port not defined in master-config")
    if not executed:
        pytest.skip("No nic defined for ports")
        
@pytest.mark.parametrize("ssh_host", ClusterInfoObj.get_memx_servers() + ClusterInfoObj.get_swarmx_servers())
def test_memlock_limit(client, ssh_host):
    _, stdout, _ = client.exec_command(f'cat /etc/security/limits.conf 2>&1')
    result = stdout.read().decode()
    rs = [l.strip() for l in result.split("\n")]
    # look for soft memlock
    found = False
    try:
        rs.index("* soft memlock unlimited")
        found = True
    except:
        pass
    
    assert found, "'soft memlock unlimited' not found"

    # look for hard memlock
    found = False
    try:
        rs.index("* hard memlock unlimited")
        found = True
    except:
        pass
    
    assert found, "'hard memlock unlimited' not found"


# Add more test functions as needed

'''

# TODO: Remove this test, it is there to just show behavior of XFAIL
@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_hostname(client, ssh_host, host_type, host_tag, ref_file_name):
        _, stdout, _ = client.exec_command('hostname')
        result = stdout.read().decode()

        status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
        assert status == 0, "Dummy Check"

@pytest.mark.test_marker
@pytest.mark.skip
@pytest.mark.parametrize("ssh_host, host_type, host_tag", ClusterInfoObj.get_all_por_servers_as_tuple())
def test_remote_command2(client, ssh_host, host_type, host_tag, ref_file_name):
        _, stdout, _ = client.exec_command('ls -la')
        result = stdout.read().decode()
        status = compare_output_regoldenize_if_needed(output_data=result, ref_file_name=ref_file_name, ssh_host=ssh_host, host_type=host_type, host_tag=host_tag)
        assert status == 0, "Check"
'''
