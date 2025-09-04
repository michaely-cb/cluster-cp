import os
import sys
import yaml
from prettytable import PrettyTable

class ClusterInfo:
    def __init__(self, name=None, config_file='/opt/cerebras/cluster/master-config.yaml'):
        self._name = name
        self._all_servers = dict()
        self._mgmt_servers = list()
        self._worker_servers = list()
        self._memx_servers = list()
        self._swarmx_servers = list()
        self._user_nodes = list()



        yaml_data = None

        if not os.path.exists(config_file):
            print(f"Warning: File '{config_file}' does not exist.")
            if name == None:
                print(f"Error: multibox name was not specified.")
                sys.exit(1)
            else:
                yaml_data = self.populate_yaml_data_from_systems_db(name)

        if yaml_data == None:
            try:
                with open(config_file, "r") as file:
                    yaml_data = yaml.safe_load(file)
            except yaml.YAMLError as e:
                    print(f"Error: Failed to load YAML file '{config_file}': {e}")
                    sys.exit(1)

        if self._name is None:
            self._name = yaml_data['name']

        for server in yaml_data['servers']:
            server_tag = ''
            server_tag_list = list()
            server_name = server['name']
            server_role = server['role']
            ip_address = server['name'] # Fall-back to DNS name if IP is not found

            if 'vendor' in server:
                server_tag_list.append(server['vendor'])

            if 'tag' in server:
                server_tag_list.append(server['tag'])

            if server_tag_list:
                server_tag = "-".join(server_tag_list)

            ports = []
            if 'ports' in server:
                ports = server["ports"]
                for connection in server['ports']:
                    if connection['name'] == 'MGMT':
                        ip_address = connection['address']
            
            # default nodeGroup is 0
            nodeGroup = 0
            if "nodeGroup" in server:
                nodeGroup = server["nodeGroup"]

            if server_role == 'MG':
                self._mgmt_servers.append(server_name)
                server_role = 'mgmt_server'
            elif server_role == 'WK':
                self._worker_servers.append(server_name)
                server_role = 'worker_server'
            elif server_role == 'MX':
                self._memx_servers.append(server_name)
                server_role = 'memx_server'
            elif server_role == 'SX':
                self._swarmx_servers.append(server_name)
                server_role = 'swarmx_server'
            elif server_role == 'US':
                self._user_nodes.append(server_name)
                server_role = 'user_node'

            self._all_servers[server_name] = dict()
            self._all_servers[server_name]['server_role'] = server_role
            self._all_servers[server_name]['server_tag'] = server_tag
            self._all_servers[server_name]['ip_address'] = ip_address
            self._all_servers[server_name]['ports'] = ports
            self._all_servers[server_name]['nodeGroup'] = nodeGroup

            if 'rootLogin' in server:
                self._all_servers[server_name]['rootLogin'] = server['rootLogin']

        #print(self._all_servers)
        #print(self._mgmt_servers)
        #print(self._worker_servers)
        #print(self._memx_servers)
        #print(self._swarmx_servers)

    def get_all_usernodes(self):
        return self._user_nodes

    def get_all_servers(self, hostnames_only=True):
        if hostnames_only:
            return self._all_servers.keys()
        return self._all_servers

    def get_all_por_servers_as_tuple(self, server_role:list=None):
        data = list()
        for server in self._all_servers:
            if server_role:
                if self._all_servers[server]['server_role'] not in server_role:
                    continue
                
            if server in self._user_nodes:
                # Doing a continue since user-nodes need to be skipped from POR testing
                continue
            tuple_data = (server, self._all_servers[server]['server_role'], self._all_servers[server]['server_tag'])
            data.append(tuple_data)
        return data

    def get_login_for_servers(self, server_role=None, hostnames=list()):
        data = list()

        for server in self._all_servers:
            tuple_data = tuple()
            if server_role is not None:
                if self._all_servers[server]['server_role'] != server_role:
                    continue
            if hostnames:
                if server not in hostnames:
                    continue

            if 'rootLogin' in self._all_servers[server]:
                tuple_data = (server, self._all_servers[server]['ip_address'], self._all_servers[server]['rootLogin']['username'], self._all_servers[server]['rootLogin']['password'])
            else:
                tuple_data = (server, self._all_servers[server]['ip_address'], 'root', 'root')
            data.append(tuple_data)
        return data

    def get_memx_servers(self):
        return self._memx_servers

    def get_swarmx_servers(self):
        return self._swarmx_servers

    def get_system_name(self):
        return self._name

    def get_cluster_info_summary_as_table(self):
        # Create a new table instance
        table = PrettyTable()
        table.field_names = ["Type of Host", "Count"]

        # Add rows of data to the table
        table.add_row(["Management Nodes", len(self._mgmt_servers)])
        table.add_row(["Worker Nodes", len(self._worker_servers)])
        table.add_row(["MemX Nodes", len(self._memx_servers)])
        table.add_row(["SwarmX Nodes", len(self._swarmx_servers)])
        table.add_row(["User Nodes", len(self._user_nodes)])

        # Set table properties (optional)
        table.border = True
        table.header_style = "upper"
        table.align = 'l'
        table.align["Count"] = 'r'

        # Return the table as string
        return(str(table))

    def get_cluster_info_detailed_as_table(self):
        # Create a new table instance
        table = PrettyTable()
        table.field_names = ["Hostname", "Type of Host", "Server Tag"]

        for server in self._all_servers:
            table.add_row([server, self._all_servers[server]['server_role'], self._all_servers[server]['server_tag']])

        # Set table properties (optional)
        table.border = True
        table.header_style = "upper"
        table.align = 'l'
        table.sortby = "Hostname"

        # Return the table as string
        return(str(table))
    
    def connect_to_systemsdb_and_get_info(self, mb):

        import requests
        import json

        url = f"http://systems-db.devinfra.cerebras.aws/api/v1/search/multiboxes?_id={mb}"

        nodeRoleList = []

        try:
            response = requests.request("GET", url)
        except:
            raise "Could not retrieve data from multibox db"
            
        mbInfo = json.loads(response.text)['data'][mb]

        if mbInfo:
            user_nodes = mbInfo['user_hosts']
            mgmt_nodes = mbInfo['management_hosts']
            memx_nodes = mbInfo['memoryx_hosts']
            swarmx_nodes = mbInfo['swarmx_hosts']
            worker_nodes = mbInfo['worker_hosts']


            for node in user_nodes:
                nodeRoleList.append({'name': node, 'role': 'US'})
            for node in mgmt_nodes:
                nodeRoleList.append({'name': node, 'role': 'MG'})
            for node in memx_nodes:
                nodeRoleList.append({'name': node, 'role': 'MX'})
            for node in swarmx_nodes:
                nodeRoleList.append({'name': node, 'role': 'SX'})
            for node in worker_nodes:
                nodeRoleList.append({'name': node, 'role': 'WK'})

        return nodeRoleList

    def populate_yaml_data_from_systems_db(self, name):

        yaml_data = {}

        yaml_data['name'] = name
        nodeRoleList = self.connect_to_systemsdb_and_get_info(name)
        yaml_data['servers'] = nodeRoleList

        return yaml_data