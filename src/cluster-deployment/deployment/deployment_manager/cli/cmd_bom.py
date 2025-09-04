import json
import os
import datetime
import re
import difflib
from collections import defaultdict
from deployment_manager.cli.subcommand import SubCommandABC
import logging
from deployment_manager.db.models import Device
from deployment_manager.tools.switch.switchctl import create_client
from deployment_manager.cli.switch.interface import _gather_switches
import time
import concurrent.futures
from deployment_manager.tools.utils import to_duration, prompt_confirm
from deployment_manager.cli.bom.scraper import get_multi_devices_bom, DeviceType
from deployment_manager.tools.config import ConfGen
from tabulate import tabulate

logger = logging.getLogger(__name__)

class Scrape(SubCommandABC):
    """ Scrape command for BOM across all device types """
    name = "scrape"

    def construct(self):
        self.parser.add_argument(
            "--no_export_excel",
            action="store_true",
            help="Skip Excel export",
            default=False
        )
        self.parser.add_argument(
            "--output_dir",
            type=str,
            help="Custom output directory for BOM data (default: log_dir/bom)",
            default=None
        )
        self.parser.add_argument(
            "--device-types",
            nargs='+',
            choices=['all', 'switches', 'servers', 'systems', 'console_servers', 'media_converters'],
            default=["all"],
            help="Device types to collect BOM information from (default: all)"
        )
    
    def get_switches_bom(self):
        """Get BOM information for switches"""
        print("Switch BOM collection not yet implemented")
        return {}
    
    def process_bom_data(self, bom):
        """Process BOM data to make it JSON serializable"""
        if hasattr(bom, '__dict__'):
            # If the object has a __dict__ attribute, use that
            bom_dict = vars(bom)
        elif hasattr(bom, '_asdict'):
            # For namedtuples
            bom_dict = bom._asdict()
        else:
            # Try to extract attributes using dir() for custom objects
            # Filter out private attributes (starting with _)
            attrs = [attr for attr in dir(bom) if not attr.startswith('_') and not callable(getattr(bom, attr))]
            bom_dict = {attr: getattr(bom, attr) for attr in attrs}
            
            # If still empty, use the object as is (assuming it's already serializable)
            if not bom_dict:
                bom_dict = bom
        
        return bom_dict

    def run(self, args):
        # Create consolidated BOM dictionary for all device types
        all_boms = {
            "switches": {},
            "servers": {},
            "systems": {},
            "console_servers": {},
            "media_converters": {}
        }
        
        # Determine which device types to collect
        device_types = args.device_types
        collect_all = "all" in device_types
        
        # Collect BOM data based on selected device types
        start_time = time.time()
        
        # Get switch BOM data (handled separately due to different interface)
        if collect_all or "switches" in device_types:
            print("Collecting BOM data for switches...")
            switch_boms = self.get_switches_bom()
            # Process switch BOM data
            for sw_name, bom in switch_boms.items():
                all_boms["switches"][sw_name] = self.process_bom_data(bom)
        
        # Use unified scraper for other device types            
        if collect_all or "console_servers" in device_types:
            print("Collecting BOM data for console servers...")
            all_boms["console_servers"] = get_multi_devices_bom(self.profile, DeviceType.CONSOLE_SERVER)

        # Use ConfGen to get the proper log directory for this profile
        confgen = ConfGen(self.profile)
        
        # Create bom directory inside the logs directory or use custom output directory
        base_dir = args.output_dir if args.output_dir else confgen.log_dir
        output_dir = os.path.join(base_dir, "bom")
        os.makedirs(output_dir, exist_ok=True)
        
        # Create subdirectories for json and excel files
        json_dir = os.path.join(output_dir, "json")
        os.makedirs(json_dir, exist_ok=True)            
        print(f"Created output directories in: {output_dir}")
        
        # Save the consolidated data to a single JSON file with date and time
        now = datetime.datetime.now()
        date_str = now.strftime("%m%d%Y")
        time_str = now.strftime("%H%M%S")
        filename_base = f"{date_str}_{time_str}_all_devices_bom"
        json_filepath = os.path.join(json_dir, f"{filename_base}.json")
        
        # Add timestamp metadata to the BOM data
        all_boms["metadata"] = {
            "date": date_str,
            "time": time_str,
            "profile": self.profile,
            "collected_device_types": device_types if not collect_all else ["all"],
            "collection_duration_seconds": time.time() - start_time
        }
        
        with open(json_filepath, 'w') as f:
            json.dump(all_boms, f, indent=4)
        print(f"Saved all BOM information to {json_filepath}")
        
        # Display summary of collected information
        device_counts = {
            "switches": len(all_boms["switches"]),
            "servers": len(all_boms["servers"]),
            "systems": len(all_boms["systems"]),
            "console_servers": len(all_boms["console_servers"]), 
            "media_converters": len(all_boms["media_converters"])
        }
        
        total_devices = sum(device_counts.values())
        elapsed_time = time.time() - start_time
        
        # Create and display summary table
        summary_data = []
        for device_type, count in device_counts.items():
            if collect_all or device_type in device_types:
                summary_data.append([device_type.capitalize(), count])
        summary_data.append(["Total", total_devices])
        
        print("\nCollection Summary:")
        print("\n" + tabulate(summary_data, headers=["Device Type", "Count"], tablefmt="grid"))
        print(f"\nTotal collection time: {to_duration(elapsed_time)}")
        
        return 0

# Update CMDS list to include the Delete command
CMDS = [Scrape]