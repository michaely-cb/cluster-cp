mock_cluster_cfg = """
kind: cluster-configuration
name: mockcluster
systems:
  - name: systemf82
    type: cs2
    managementAddress: 172.28.0.79
    controlAddress: "10.252.47.208:9000"
  - name: systemf47
    type: cs2
    managementAddress: 172.28.0.63
    controlAddress: "10.252.47.224:9000"
nodes:
  - name: sc-r11r9-s2
    role: worker
    properties:
      group: ng0
  - name: sc-r11r9-s11
    role: broadcastreduce
    networkInterfaces:
      - name: enp197s0f0
        csPort: 0
        address: 10.252.32.176
      - name: enp197s0f1
        csPort: 0
        address: 10.252.32.192
      - name: enp1s0f1
        address: 10.252.32.128
        csPort: 0
      - name: enp129s0f0
        csPort: 1
        address: 10.252.32.144
      - name: enp129s0f1
        csPort: 1
        address: 10.252.32.160
      - name: enp1s0f0
        address: 10.252.32.112
        csPort: 1
  - name: sc-r11r9-s112
    role: broadcastreduce
  - name: sc-r11r9-s113
    role: broadcastreduce
  - name: sc-r11r9-s114
    role: broadcastreduce
  - name: sc-r11r9-s115
    role: broadcastreduce
  - name: sc-r11r9-s116
    role: broadcastreduce
  - name: sc-r11r9-s15
    role: memory
    properties:
      group: ng0
  - name: sc-r11r9-s00
    role: invalid
groups:
- name: ng0
  properties:
    memory: 1TB
"""

mock_devinfra_cluster = {
    "_id": "test-test",
    "aw_switches": ["cs-mx-sw-01-01", "cs-mx-sw-02-01"],
    "location": "Colovore",
    "management_hosts": ["cs-mg-sr-01-01"],
    "memoryx_hosts": ["cs-mx-sr-01-01", "cs-mx-sr-01-02", ],
    "name": "test-test",
    "purpose": "",
    "swarmx_hosts": ["cs-sx-sr-01-01", "cs-sx-sr-01-02", ],
    "systems": ["systemf1", "systemf2"],
    "type": "Multibox",
    "user_hosts": ["cs-us-sr-01-01"],
    "worker_hosts": ["cs-wk-sr-01-01", "cs-wk-sr-01-02", ]
}
