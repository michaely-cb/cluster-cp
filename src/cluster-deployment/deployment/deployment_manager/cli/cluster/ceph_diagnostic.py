#!/usr/bin/env python3
"""
Standalone Ceph connectivity diagnostic command.

Usage:
python ceph_diagnostic.py <profile> <src_cluster> <dst_cluster>

This runs comprehensive connectivity tests between management nodes and their respective
Ceph monitors to help identify why some clusters can reach their monitors while others cannot.
"""

import argparse
import logging
import sys

from deployment_manager.cli.cluster.ceph_sync.diagnose_ceph_connectivity import run_diagnostic

def main():
    parser = argparse.ArgumentParser(description='Diagnose Ceph connectivity issues between clusters')
    parser.add_argument('profile', help='Deployment profile name')
    parser.add_argument('src_cluster', help='Source cluster name')
    parser.add_argument('dst_cluster', help='Destination cluster name')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--output', '-o', choices=['summary', 'json'], default='summary',
                       help='Output format (default: summary)')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        results = run_diagnostic(args.profile, args.src_cluster, args.dst_cluster)
        
        if args.output == 'json':
            import json
            print(json.dumps(results, indent=2, default=str))
        
        return 0
        
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())