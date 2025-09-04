#!/usr/bin/env python3
"""
Diagnostic script to analyze Ceph connectivity issues between management nodes and Ceph monitors.

This script helps identify why source cluster management nodes might not be able to reach
their own Ceph monitors while destination management nodes can reach theirs.
"""

import json
import logging
import re
import sys
from typing import Dict, List, Tuple

from deployment_manager.tools.kubernetes.interface import KubernetesCtl
from deployment_manager.tools.kubernetes.utils import get_k8s_lead_mgmt_node
from deployment_manager.db import device_props as props

logger = logging.getLogger(__name__)

class CephConnectivityDiagnostic:
    """Diagnose Ceph connectivity issues between management nodes and monitors."""
    
    def __init__(self, profile: str):
        self.profile = profile
    
    def _extract_monitors_from_config(self, ceph_conf_content: str) -> List[str]:
        """Extract monitor IPs and ports from ceph.conf."""
        mon_match = re.search(r'mon_host\s*=\s*(.+)', ceph_conf_content)
        if not mon_match:
            return []
        
        mon_string = mon_match.group(1).strip()
        # Split by comma and clean up each monitor
        monitors = [mon.strip() for mon in mon_string.split(',') if mon.strip()]
        return monitors
    
    def _test_connectivity(self, kctl: KubernetesCtl, target_ip: str, target_port: int, 
                          test_name: str) -> Dict[str, any]:
        """Test network connectivity to a specific IP and port."""
        result = {
            'target': f"{target_ip}:{target_port}",
            'test_name': test_name,
            'success': False,
            'details': {}
        }
        
        # Test 1: Basic ping
        ret, ping_out, ping_err = kctl._conn.exec(f"ping -c 3 -W 2 {target_ip}")
        result['details']['ping'] = {
            'success': ret == 0,
            'output': ping_out if ret == 0 else ping_err
        }
        
        # Test 2: TCP connectivity
        ret, tcp_out, tcp_err = kctl._conn.exec(
            f"timeout 5 bash -c 'echo >/dev/tcp/{target_ip}/{target_port}' 2>/dev/null"
        )
        result['details']['tcp'] = {
            'success': ret == 0,
            'output': tcp_out if ret == 0 else tcp_err
        }
        
        # Test 3: Netcat if available
        ret, nc_out, nc_err = kctl._conn.exec(f"timeout 5 nc -zv {target_ip} {target_port}")
        result['details']['netcat'] = {
            'success': ret == 0,
            'output': nc_out if ret == 0 else nc_err
        }
        
        # Test 4: Traceroute to see routing path
        ret, trace_out, trace_err = kctl._conn.exec(f"timeout 10 traceroute -m 5 {target_ip}")
        result['details']['traceroute'] = {
            'success': ret == 0,
            'output': trace_out if ret == 0 else trace_err
        }
        
        # Test 5: Check local routing table
        ret, route_out, _ = kctl._conn.exec(f"ip route get {target_ip}")
        result['details']['routing'] = {
            'success': ret == 0,
            'output': route_out if ret == 0 else "No route found"
        }
        
        # Overall success if TCP connection works
        result['success'] = result['details']['tcp']['success']
        return result
    
    def _get_cluster_network_info(self, kctl: KubernetesCtl, cluster_name: str) -> Dict[str, any]:
        """Get network information about the cluster."""
        info = {
            'cluster_name': cluster_name,
            'network_interfaces': {},
            'kubernetes_services': {},
            'ceph_pods': {},
            'cluster_config': {}
        }
        
        # Get network interfaces
        ret, iface_out, _ = kctl._conn.exec("ip addr show")
        if ret == 0:
            info['network_interfaces']['host'] = iface_out
        
        # Get Kubernetes service information
        ret, svc_out, _ = kctl._conn.exec("kubectl get svc -A -o wide")
        if ret == 0:
            info['kubernetes_services']['all'] = svc_out
            
        # Get Ceph-specific services
        ret, ceph_svc_out, _ = kctl._conn.exec("kubectl get svc -n rook-ceph -o json")
        if ret == 0:
            try:
                info['kubernetes_services']['ceph'] = json.loads(ceph_svc_out)
            except:
                info['kubernetes_services']['ceph'] = ceph_svc_out
        
        # Get Ceph pod information
        ret, ceph_pods_out, _ = kctl._conn.exec("kubectl get pods -n rook-ceph -o wide")
        if ret == 0:
            info['ceph_pods']['status'] = ceph_pods_out
            
        # Get Ceph monitor endpoints
        ret, mon_endpoints, _ = kctl._conn.exec(
            "kubectl get endpoints -n rook-ceph -l app=rook-ceph-mon -o json"
        )
        if ret == 0:
            try:
                info['ceph_pods']['monitor_endpoints'] = json.loads(mon_endpoints)
            except:
                info['ceph_pods']['monitor_endpoints'] = mon_endpoints
        
        # Get cluster configuration
        ret, cluster_info, _ = kctl._conn.exec("kubectl cluster-info")
        if ret == 0:
            info['cluster_config']['cluster_info'] = cluster_info
            
        return info
    
    def _fetch_ceph_credentials(self, kctl: KubernetesCtl, cluster_name: str) -> Tuple[str, str]:
        """Fetch Ceph credentials from the cluster."""
        logger.info(f"Fetching Ceph credentials from {cluster_name}...")
        
        # Get ceph.conf
        ret, conf_content, err = kctl._conn.exec(
            "kubectl exec deploy/rook-ceph-tools -n rook-ceph -- cat /etc/ceph/ceph.conf"
        )
        if ret != 0:
            raise RuntimeError(f"Failed to fetch ceph.conf from {cluster_name}: {err}")
        
        # Get keyring
        ret, keyring_content, err = kctl._conn.exec(
            "kubectl exec deploy/rook-ceph-tools -n rook-ceph -- ceph auth get client.admin"
        )
        if ret != 0:
            raise RuntimeError(f"Failed to fetch keyring from {cluster_name}: {err}")
        
        return conf_content, keyring_content
    
    def diagnose_cluster(self, cluster_name: str) -> Dict[str, any]:
        """Run comprehensive diagnostics on a single cluster."""
        logger.info(f"=== Diagnosing cluster: {cluster_name} ===")
        
        mgmt_node = get_k8s_lead_mgmt_node(self.profile, cluster_name)
        user = mgmt_node.get_prop(props.prop_management_credentials_user)
        password = mgmt_node.get_prop(props.prop_management_credentials_password)
        
        cluster_result = {
            'cluster_name': cluster_name,
            'management_node': mgmt_node.name,
            'success': False,
            'error': None,
            'network_info': {},
            'ceph_credentials': {},
            'connectivity_tests': []
        }
        
        try:
            with KubernetesCtl(self.profile, mgmt_node.name, user, password) as kctl:
                # 1. Get basic network information
                cluster_result['network_info'] = self._get_cluster_network_info(kctl, cluster_name)
                
                # 2. Fetch Ceph credentials
                conf_content, keyring_content = self._fetch_ceph_credentials(kctl, cluster_name)
                cluster_result['ceph_credentials'] = {
                    'conf_size': len(conf_content),
                    'keyring_size': len(keyring_content),
                    'conf_content': conf_content,
                    'keyring_content': keyring_content
                }
                
                # 3. Extract monitors and test connectivity
                monitors = self._extract_monitors_from_config(conf_content)
                logger.info(f"Found {len(monitors)} monitors in {cluster_name}: {monitors}")
                
                for monitor in monitors:
                    if ':' in monitor:
                        mon_ip, mon_port = monitor.split(':', 1)
                        mon_port = int(mon_port)
                    else:
                        mon_ip = monitor
                        mon_port = 6789  # Default Ceph monitor port
                    
                    test_result = self._test_connectivity(
                        kctl, mon_ip, mon_port, 
                        f"Monitor {monitor} from {mgmt_node.name}"
                    )
                    cluster_result['connectivity_tests'].append(test_result)
                
                # 4. Test connectivity to Kubernetes services
                if 'ceph' in cluster_result['network_info']['kubernetes_services']:
                    ceph_services = cluster_result['network_info']['kubernetes_services']['ceph']
                    if isinstance(ceph_services, dict) and 'items' in ceph_services:
                        for service in ceph_services['items']:
                            if 'mon' in service.get('metadata', {}).get('name', ''):
                                svc_ip = service.get('spec', {}).get('clusterIP')
                                if svc_ip and svc_ip != 'None':
                                    test_result = self._test_connectivity(
                                        kctl, svc_ip, 6789,
                                        f"ClusterIP service {service['metadata']['name']}"
                                    )
                                    cluster_result['connectivity_tests'].append(test_result)
                
                cluster_result['success'] = True
                
        except Exception as e:
            cluster_result['error'] = str(e)
            logger.error(f"Failed to diagnose {cluster_name}: {e}")
        
        return cluster_result
    
    def compare_clusters(self, src_cluster_name: str, dst_cluster_name: str) -> Dict[str, any]:
        """Compare connectivity between source and destination clusters."""
        logger.info("=== Starting cross-cluster connectivity comparison ===")
        
        src_result = self.diagnose_cluster(src_cluster_name)
        dst_result = self.diagnose_cluster(dst_cluster_name)
        
        comparison = {
            'source': src_result,
            'destination': dst_result,
            'analysis': {
                'src_monitors_reachable': 0,
                'dst_monitors_reachable': 0,
                'src_total_monitors': 0,
                'dst_total_monitors': 0,
                'issues_found': [],
                'recommendations': []
            }
        }
        
        # Analyze results
        if src_result['success']:
            comparison['analysis']['src_total_monitors'] = len(src_result['connectivity_tests'])
            comparison['analysis']['src_monitors_reachable'] = sum(
                1 for test in src_result['connectivity_tests'] if test['success']
            )
        
        if dst_result['success']:
            comparison['analysis']['dst_total_monitors'] = len(dst_result['connectivity_tests'])
            comparison['analysis']['dst_monitors_reachable'] = sum(
                1 for test in dst_result['connectivity_tests'] if test['success']
            )
        
        # Generate analysis
        issues = comparison['analysis']['issues_found']
        recommendations = comparison['analysis']['recommendations']
        
        if comparison['analysis']['src_monitors_reachable'] == 0:
            issues.append("Source cluster: NO monitors reachable from management node")
            recommendations.append("Check source cluster network configuration and Ceph service status")
        
        if comparison['analysis']['dst_monitors_reachable'] == 0:
            issues.append("Destination cluster: NO monitors reachable from management node")
            recommendations.append("Check destination cluster network configuration and Ceph service status")
        
        if (comparison['analysis']['dst_monitors_reachable'] > 0 and 
            comparison['analysis']['src_monitors_reachable'] == 0):
            issues.append("Asymmetric connectivity: Destination works but source doesn't")
            recommendations.append("Compare network configurations between clusters")
            recommendations.append("Check if source cluster has different Ceph deployment method")
        
        if (comparison['analysis']['src_monitors_reachable'] > 0 and 
            comparison['analysis']['dst_monitors_reachable'] > 0):
            issues.append("Both clusters have working connectivity - investigate CephFS mounting specifically")
            recommendations.append("Test CephFS mounting manually on both management nodes")
        
        return comparison
    
    def print_summary(self, comparison_result: Dict[str, any]):
        """Print a human-readable summary of the diagnostic results."""
        print("\n" + "="*80)
        print("CEPH CONNECTIVITY DIAGNOSTIC SUMMARY")
        print("="*80)
        
        analysis = comparison_result['analysis']
        
        print(f"\nSource Cluster Connectivity:")
        print(f"  - Monitors reachable: {analysis['src_monitors_reachable']}/{analysis['src_total_monitors']}")
        
        print(f"\nDestination Cluster Connectivity:")
        print(f"  - Monitors reachable: {analysis['dst_monitors_reachable']}/{analysis['dst_total_monitors']}")
        
        if analysis['issues_found']:
            print(f"\nISSUES IDENTIFIED:")
            for i, issue in enumerate(analysis['issues_found'], 1):
                print(f"  {i}. {issue}")
        
        if analysis['recommendations']:
            print(f"\nRECOMMENDATIONS:")
            for i, rec in enumerate(analysis['recommendations'], 1):
                print(f"  {i}. {rec}")
        
        print(f"\nDETAILED RESULTS:")
        print(f"Use the returned data structure for detailed connectivity test results.")
        print("="*80)


def run_diagnostic(profile: str, src_cluster: str, dst_cluster: str):
    """Run the diagnostic and return results."""
    diagnostic = CephConnectivityDiagnostic(profile)
    results = diagnostic.compare_clusters(src_cluster, dst_cluster)
    diagnostic.print_summary(results)
    return results


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python diagnose_ceph_connectivity.py <profile> <src_cluster> <dst_cluster>")
        sys.exit(1)
    
    profile, src_cluster, dst_cluster = sys.argv[1:4]
    run_diagnostic(profile, src_cluster, dst_cluster)