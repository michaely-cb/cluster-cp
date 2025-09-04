#!/bin/bash

# Typical use cse: user node talks to mgmt-node-100G-ip:443 with hostname pointing the underline service like CRD.
# When user node reside in the same subnet of the parent data network, it will cause hang.
# It should not happen by design since user node are expected outside of cluster, but does happen internally mostly.
# https://github.com/Cerebras/monolith/pull/77244#issue-1722862192
#
# Add an SNAT of local 1G ip rule for traffic incoming on the 100G network dest nginx's port 443 as workaround.
#
# Without it, when packets routing back to user node from nginx with multus setup will go through net1 instead of eth0
# which will bypass dnat/snat and usernode will see the nginx multus ip instead of the original mgmt node 100G ip
# which will cause hang since user node client is waiting for packets with the original dst ip.
#
# With it, extra rule will enforce the snat/dnat happens that converts to non-100G ip
# so nginx pod routing will still go with the default eth0 to apply dnat/snat from k8s svc and ensure client still mgmt node ip.
#
# The key idea is that we only need the ability of keep traffic local but we don't care about preserving client ip.
# With this workaround, nginx pod will not see the client ip but keep ability of local traffic.
# https://kubernetes.io/docs/tutorials/services/source-ip/

set -e

# These variables are set by the installation script:
DATA_PARENTNET=SET_DATA_PARENTNET
# (HOSTNAME,IP;)+
MGMT_IP_MAP="SET_MGMT_IP_MAP"

MY_NAME=$(hostname -s)
IFS=';'
for pair in $MGMT_IP_MAP ; do
  if [[ -n "$pair" ]]; then
    name=$(cut -d, -f1 <<< "$pair")
    if [[ "$name" == "$MY_NAME" ]]; then
      HOST_IP=$(cut -d, -f2 <<< "$pair")
      break
    fi
  fi
done

if [ -z "$HOST_IP" ] ; then
  echo "failed to find $MY_NAME in $MGMT_IP_MAP"
  exit 1
fi

echo "masquerading inbound traffic on port 443 to local IP ${HOST_IP}"

COMMENT="external 100G nginx traffic SNAT"

get_rule_index() {
    local rule_line=$(iptables -t nat -L POSTROUTING --line-numbers 2>/dev/null | grep SNAT | grep "$COMMENT")
    if [ -z "$rule_line" ] ; then return 1 ; fi
    awk '{print $1}' <<< "$rule_line"
}

trap 'echo "Signal caught, script exiting"; exit 0' SIGINT SIGTERM

if get_rule_index &>/dev/null ; then
    # clear the rule before re-adding it in case any parameters of the rule changed
    echo "Refreshing rule"
    iptables -t nat -D POSTROUTING $(get_rule_index)
else
    echo "Adding rule"
fi
iptables -t nat -A POSTROUTING -p tcp -s $DATA_PARENTNET --dport 443 -j SNAT --to-source $HOST_IP -m comment --comment "$COMMENT"

echo "If uninstalling permanently, you can manually remove the rule with"
echo "  iptables -t nat -D POSTROUTING $(get_rule_index)"
echo "or by restarting"
echo ""
echo "Ensuring rule not removed..."
while get_rule_index &>/dev/null ; do
    sleep 5
done
echo "Rule removed, restarting to re-create rule"
exit 1