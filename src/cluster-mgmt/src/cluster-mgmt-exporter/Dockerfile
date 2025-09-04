ARG TAG="default"
ARG BASE=python:3.11-slim-bookworm
FROM ${BASE}

RUN apt-get update && apt-get install bash openssh-client sshpass ethtool iputils-ping mstflint pciutils systemd yq jq -y

ADD http://sc-r9ra14-s9.cerebrassc.local/bin/niccli_234.0.151.0-1_x86_64.deb ./
RUN dpkg -i niccli_234.0.151.0-1_x86_64.deb
RUN rm niccli_234.0.151.0-1_x86_64.deb

ADD requirements.txt ./requirements.txt
RUN pip install -r ./requirements.txt

COPY system_exporter*.py ./
COPY node_exporter.py ./node_exporter.py
COPY redfish_fetcher.py ./redfish_fetcher.py

# linkmon
COPY linkmon_ethtool_only.py ./linkmon_ethtool_only.py
COPY csx.py ./csx.py
COPY node.py ./node.py
COPY nodeinfo.py ./nodeinfo.py
COPY linkmon.py ./linkmon.py
COPY generate_interface_list.py ./generate_interface_list.py
COPY switch.py ./switch.py
COPY arista_switch_fast_polling.py ./arista_switch_fast_polling.py
COPY switch_utils.py ./switch_utils.py
COPY juniper.py ./juniper.py
COPY credo.py ./credo.py

# interface_watch_list server
COPY interface_watch_list_server.py ./interface_watch_list_server.py

# ipmi
COPY node_ipmi_exporter.py ./node_ipmi_exporter.py

# system client
COPY system_client ./system_client
# protos
COPY pb ./pb

ENV PYTHONPATH="${PYTHONPATH}:/pb"

