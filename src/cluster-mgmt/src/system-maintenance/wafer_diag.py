#!/usr/bin/env python3
"""System maintenance wrapper for wafer-diag execution."""
import sys
import asyncio
import argparse 
import subprocess
import os
import time

# Path setup - prioritize framework without breaking system
def setup_framework_paths():    
    """Set up Python paths for system maintenance framework"""
    framework_dir = '/system-maintenance-framework'
    pb_dir = f'{framework_dir}/pb'
    
    # Check if paths exist
    if not os.path.exists(framework_dir):
        raise ImportError(f"Framework directory not found: {framework_dir}")
    if not os.path.exists(pb_dir):
        raise ImportError(f"Protobuf directory not found: {pb_dir}")
    
    # Hide the system schema package to avoid conflicts
    if 'schema' in sys.modules:
        del sys.modules['schema']
        print("Removed conflicting system schema module")
    
    # Remove any existing framework paths to avoid duplicates
    sys.path = [p for p in sys.path if not p.startswith(framework_dir)]
    
    # Insert framework paths at the beginning (high precedence)
    # but after the script's directory (sys.path[0])
    sys.path.insert(1, pb_dir)      # Protobuf modules get priority
    sys.path.insert(2, framework_dir) # Framework modules
    
    print(f"Framework paths added: {pb_dir}, {framework_dir}")

# Set up paths before any imports
setup_framework_paths()

# Test critical imports to fail fast
try:
    from system_maintenance_helper import SystemMaintenanceExecutionFailedError
    print("✓ Framework import successful")
except ImportError as e:
    print(f"✗ Framework import failed: {e}")
    print("Available paths:")
    for i, path in enumerate(sys.path[:10]):  # Show first 10 paths
        print(f"  {i}: {path}")
    sys.exit(1)

from system_maintenance_helper import (
    collect_system_status,
    assert_system_maintenance_status,
    assert_system_health_status,
    get_system_credentials,
    SystemMaintenanceExecutionFailedError,
    DRYRUN_SLEEP_SECONDS,
)

from system_client.tunnel_system_client import TunneledSystemClient
from system_client.system_exporter_logger import logger

# Constants
WAFER_DIAG_EXECUTABLE = "/cbcore/bin/wafer-diag"

def get_cm_address():
    """Get CM address from environment variable set by server"""
    cm_addr = os.getenv("CM_ADDRESS")
    if not cm_addr:
        raise SystemMaintenanceExecutionFailedError("CM_ADDRESS environment variable not set")
    return cm_addr

def build_wafer_diag_command(cm_addr, args):
    """Build wafer-diag command with all specified options"""
    cmd = [WAFER_DIAG_EXECUTABLE, "--cmaddr", str(cm_addr)]

    if args.variants:
        cmd.append("--variant")
        variant_list = [v.strip() for v in args.variants.split(',') if v.strip()]
        cmd.extend(variant_list)
            
    if args.duration:
        cmd.extend(["--duration", str(args.duration)])
            
    if args.sleep_scale:
        cmd.extend(["--sleep-scale", str(args.sleep_scale)])
            
    # Boolean flags
    boolean_flags = [
        (args.memconfig, "--memconfig"),
        (args.no_math, "--no-math"),
        (args.continue_after_fail, "--continue-after-fail"),
        (args.coredump_on_fail, "--coredump-on-fail"),
        (args.skip_config_api_check, "--skip-config-api-check"),
    ]
    
    for condition, flag in boolean_flags:
        if condition:
            cmd.append(flag)
    
    logger.info(f"Built wafer-diag command: {' '.join(cmd)}")
    return cmd

async def perform_system_check(system_name, system_client, check_type, allow_error):
    """Perform system status check (pre or post)"""
    logger.info(f"Starting {check_type.lower()}-check for system {system_name}")
    
    system_status = await collect_system_status(system_name, system_client)
    if system_status is None:
        error_msg = f"System {check_type.lower()}-check failed: unable to retrieve system status"
        logger.error(error_msg)
        raise SystemMaintenanceExecutionFailedError(error_msg)

    logger.info(f"System status summary from {check_type.lower()}-check:\n{system_status}")
        
    if not allow_error:
        assert_system_health_status(system_status)
        assert_system_maintenance_status(system_status, expect_maintenance_enabled=False)

async def run_wafer_diag_native(cm_addr, args):
    """Run wafer-diag with all specified options"""
    cmd = build_wafer_diag_command(cm_addr, args)
    
    username, password = get_system_credentials()
    tunnel_client = TunneledSystemClient(args.system_name, args.system_addr, username, password)
        
    try:
        logger.info(f"Connecting to system {args.system_name}")
        await tunnel_client.connect()
        system_client = tunnel_client.system_client
        
        if not system_client:
            error_msg = "Unable to retrieve system_grpc_client"
            logger.error(error_msg)
            raise SystemMaintenanceExecutionFailedError(error_msg)
        
        logger.info("System client connection established")
        
        # Pre-check
        await perform_system_check(args.system_name, system_client, "Pre", args.allow_error)
        
        logger.info("=" * 20)
        logger.info("=== STARTING WAFER-DIAG EXECUTION ===")
        logger.info(f"Command: {' '.join(cmd)}")
        logger.info(f"Working directory: {os.getcwd()}")
        logger.info(f"Environment CM_ADDRESS: {os.getenv('CM_ADDRESS')}")
        logger.info("=" * 20)
        
        # Ensure output is flushed before subprocess
        sys.stdout.flush()
        sys.stderr.flush()
        
        # Execute cbcore/bin/wafer-diag
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=False
        )
        
         # Ensure any remaining output is flushed
        sys.stdout.flush()
        sys.stderr.flush()
        
        
        logger.info("=" * 20)
        logger.info("=== WAFER-DIAG EXECUTION COMPLETED ===")
        logger.info(f"Exit code: {result.returncode}")
        logger.info("=" * 20)
        
        # Evaluate results
        wafer_diag_success = result.returncode == 0
        if wafer_diag_success:
            logger.info("Wafer-diag execution successful")
        else:
            logger.error(f"Wafer-diag failed with exit code {result.returncode}")
            
        # Post-check
        await perform_system_check(args.system_name, system_client, "Post", args.allow_error)
        
        return wafer_diag_success

    except FileNotFoundError:
        error_msg = (
            f"Wafer-diag executable not found at {WAFER_DIAG_EXECUTABLE}. "
            "Ensure container image is cbcore and path is mounted."
        )
        logger.error(error_msg)
        return False
    
    except Exception as e:
        error_msg = f"Failed to run wafer-diag: {e}"
        logger.error(error_msg)
        if not args.allow_error:
            raise
        return False
    finally:
        if tunnel_client:
            try:
                await tunnel_client.close()
                logger.info("Tunnel client closed")
            except Exception as e:
                error_msg = f"Failed to close tunnel client: {e}"
                logger.error(error_msg)

async def main():
    parser = argparse.ArgumentParser(description="System maintenance wrapper for wafer-diag execution")
    
    # Positional arguments
    parser.add_argument("system_name", help="Target system name")
    parser.add_argument("system_addr", help="Target system address (compatibility)")
    
    # Wafer-diag options
    parser.add_argument("--variants", help="Comma-separated list of variants")
    parser.add_argument("--duration", type=float, help="Duration for wafer-diag execution")
    parser.add_argument("--sleep-scale", type=float, help="Sleep scale factor")
    parser.add_argument("--memconfig", action="store_true", help="Enable memconfig")
    parser.add_argument("--no-math", action="store_true", help="Disable math operations")
    parser.add_argument("--continue-after-fail", action="store_true", help="Continue after failure")
    parser.add_argument("--coredump-on-fail", action="store_true", help="Generate coredump on failure")
    parser.add_argument("--skip-config-api-check", action="store_true", help="Skip configuration API check")
    parser.add_argument("--allow-error", action="store_true", help="Allow errors during execution")

    args = parser.parse_args()

    # Get CM address
    try:
        cm_addr = get_cm_address()
        logger.info(f"CM Address: {cm_addr}")
    except SystemMaintenanceExecutionFailedError as e:
        if args.allow_error:
            logger.error(f"{e}, but allow-error is enabled")
            return 1
        raise

    # Execute wafer-diag
    try:
        # Log execution info
        logger.info("=== Cbcore Native Wafer-Diag System Maintenance ===")
        logger.info(f"System: {args.system_name}")
        logger.info(f"CM Address: {cm_addr}")
        logger.info(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                
        if args.variants:
            logger.info(f"Running variants: {args.variants}")
        else:
            logger.info("Running default variants")
        
        success = await run_wafer_diag_native(cm_addr, args)
        
        if success:
            logger.info("=== Wafer-diag PASSED ===")
            return 0
        else:
            logger.error("=== Wafer-diag FAILED ===")
            return 1

    except Exception as e:
        logger.error(f"Unexpected error during wafer-diag execution: {e}")
        return 1


if __name__ == "__main__":
    logger.info("Starting wafer-diag system maintenance...")
    
    # Check for dry-run mode
    if os.getenv("DRYRUN_MODE") == "true":
        logger.info("Dry run mode enabled. Skipping actual execution.")
        sleep_duration = int(os.getenv("DRYRUN_SLEEP_SECONDS", str(DRYRUN_SLEEP_SECONDS)))
        time.sleep(sleep_duration)
        logger.info("Dry run completed")
        sys.exit(0)
    
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
