# conftest.py

import paramiko
import pytest
from py.xml import html
from common_init import ClusterInfoObj

#####################################################
# All the fixtures
#####################################################

@pytest.fixture(scope='session')
def ssh_clients(request):
    clients = {}

    collected_tests = request.session.items

    hosts_to_ssh = dict()
    for test_name in collected_tests:
        test_name = str(test_name)
        if "[" in test_name and "]" in test_name:
            test_param = test_name.split("[", 1)[1].rsplit("]", 1)[0]
            if test_param not in hosts_to_ssh:
                hosts_to_ssh[test_param] = None

    # Create SSH connections and append clients to the list
    ssh_details = ClusterInfoObj.get_login_for_servers()

    for ssh_host, ssh_ip_address, ssh_username, ssh_password in ssh_details:
        skip_flag = True
        for host_string_check in hosts_to_ssh.keys():
            if ssh_host in host_string_check or ssh_host == host_string_check:
                skip_flag = False
        if skip_flag:
            continue

        retry_count = 1
        while retry_count > 0:
            try:
                #print(f"Logging into {ssh_host}")
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(ssh_ip_address, username=ssh_username, password=ssh_password, banner_timeout=30)
                clients[ssh_host] = client
                break
            except Exception as e:
                clients[ssh_host] = None
                print(f"Error in sshing to {ssh_host} - {e}")
                #time.sleep(2)

            retry_count -= 1

    yield clients

    # Teardown: Close all SSH connections
    for client in clients:
        if clients[client] is not None:
            clients[client].close()

@pytest.fixture(scope='function')
def client(ssh_clients, ssh_host):
    if ssh_clients[ssh_host] is None:
        raise Exception(f"Check whether {ssh_host} is reachable, and if its login credentials are proper.")
    yield ssh_clients[ssh_host]

@pytest.fixture
def ref_file_name(request):
    full_name = request.node.name
    name_without_prefix = full_name.split("test_", 1)[-1]
    name_without_parameters = name_without_prefix.split("[", 1)[0]
    return name_without_parameters

#####################################################
# Changes for HTML report to have additional columns
#####################################################

def pytest_html_results_table_header(cells):
    cells.insert(2, html.th('HostName', **{'class': 'sortable'}))
    cells.insert(3, html.th('Server-Type', **{'class': 'sortable'}))
    cells.insert(4, html.th('Tag', **{'class': 'sortable'}))

def pytest_html_results_table_row(report, cells):
    test_id = report.nodeid.encode("utf-8").decode("unicode_escape")

    if "[" in test_id and "]" in test_id:
        parameter = test_id.split("[", 1)[1].rsplit("]", 1)[0]
    else:
        parameter = ""

    split_parts = parameter.rsplit('-', 2)
    if len(split_parts) == 3:
        hostname, host_type, tag = split_parts
    else:
        hostname = ''
        tag = ''
        host_type = ''
    cells.insert(2, html.td(str(hostname)))
    cells.insert(3, html.td(str(host_type)))
    cells.insert(4, html.td(str(tag)))
