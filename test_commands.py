#!/usr/bin/env python3
"""
OpenEO Bench Command Testing Script

This script tests all commands defined in the "Usage" section of README.md.
It executes each command using subprocess and logs the results to a file.
"""

import subprocess
import datetime
import logging
import os
import sys
import tempfile
import shutil
from pathlib import Path

# Setup logging
log_filename = f"command_test_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class CommandTester:
    """Class to test OpenEO Bench commands"""
    
    def __init__(self):
        self.base_dir = Path.cwd()
        self.test_dir = self.base_dir / "test_output"
        self.setup_test_environment()
        
        # Define all commands to test
        self.commands = [
            # Help commands
            {
                "name": "Help",
                "cmd": ["python", "openeobench", "--help"],
                "description": "Display help information",
                "skip_if_missing": []
            },
            
            # Service commands
            {
                "name": "Service check from CSV",
                "cmd": ["python", "openeobench", "service", "-i", "backends.csv", "-o", str(self.test_dir / "service_results")],
                "description": "Check endpoints from CSV file",
                "skip_if_missing": ["backends.csv"]
            },
            {
                "name": "Service check from CSV with append",
                "cmd": ["python", "openeobench", "service", "-i", "backends.csv", "-o", str(self.test_dir / "service_results"), "--append"],
                "description": "Check endpoints from CSV file with append",
                "skip_if_missing": ["backends.csv"]
            },
            {
                "name": "Service check single URL",
                "cmd": ["python", "openeobench", "service", "-u", "https://openeo.dataspace.copernicus.eu/.well-known/openeo", "-o", str(self.test_dir / "service_results")],
                "description": "Check single URL",
                "skip_if_missing": []
            },
            {
                "name": "Service check single URL with append",
                "cmd": ["python", "openeobench", "service", "-u", "https://openeo.dataspace.copernicus.eu/.well-known/openeo", "-o", str(self.test_dir / "service_results"), "--append"],
                "description": "Check single URL with append",
                "skip_if_missing": []
            },
            
            # Service summary commands
            {
                "name": "Service summary from folder (CSV)",
                "cmd": ["python", "openeobench", "service-summary", "-i", "outputs/", "-o", str(self.test_dir / "summary.csv")],
                "description": "Generate service summary from outputs folder to CSV",
                "skip_if_missing": ["outputs/"]
            },
            {
                "name": "Service summary from folder (MD)",
                "cmd": ["python", "openeobench", "service-summary", "-i", "outputs/", "-o", str(self.test_dir / "summary.md")],
                "description": "Generate service summary from outputs folder to Markdown",
                "skip_if_missing": ["outputs/"]
            },
            {
                "name": "Service summary from single CSV",
                "cmd": ["python", "openeobench", "service-summary", "-i", "outputs/2025-06-26.csv", "-o", str(self.test_dir / "single_summary.md")],
                "description": "Generate service summary from single CSV file",
                "skip_if_missing": ["outputs/2025-06-26.csv"],
                "alternative_files": ["outputs/2025-06-21.csv", "outputs/2025-06-20.csv", "outputs/2025-06-19.csv", "outputs/2025-06-18.csv"]
            },
            
            # Run commands
            {
                "name": "Run scenario (help)",
                "cmd": ["python", "openeobench", "run", "--help"],
                "description": "Display run command help",
                "skip_if_missing": []
            },
            {
                "name": "Run scenario",
                "cmd": ["python", "openeobench", "run", "--api-url", "https://openeo.dataspace.copernicus.eu", "-i", "scenarios/bratislava_10km_2018_openeo_platform.json", "-o", str(self.test_dir / "run_results")],
                "description": "Run OpenEO scenario on Copernicus Data Space backend",
                "skip_if_missing": ["scenarios/bratislava_10km_2018_openeo_platform.json"]
            },
            
            # Run summary commands
            {
                "name": "Run summary",
                "cmd": ["python", "openeobench", "run-summary", "-i", str(self.test_dir / "mock_output1"), str(self.test_dir / "mock_output2"), "-o", str(self.test_dir / "timing_summary.csv")],
                "description": "Generate timing summary from run results",
                "skip_if_missing": [],
                "setup_mock_data": True
            },
            
            # Result summary commands
            {
                "name": "Result summary (CSV)",
                "cmd": ["python", "openeobench", "result-summary", str(self.test_dir / "mock_output1"), str(self.test_dir / "mock_output2"), "--output", str(self.test_dir / "file_stats.csv")],
                "description": "Generate file statistics summary (CSV)",
                "skip_if_missing": [],
                "setup_mock_data": True
            },
            {
                "name": "Result summary (MD)",
                "cmd": ["python", "openeobench", "result-summary", str(self.test_dir / "mock_output1"), str(self.test_dir / "mock_output2"), "--output", str(self.test_dir / "file_stats.md"), "--format", "md"],
                "description": "Generate file statistics summary (Markdown)",
                "skip_if_missing": [],
                "setup_mock_data": True
            },
            
            # Process commands
            {
                "name": "Process compliance single backend",
                "cmd": ["python", "openeobench", "process", "--url", "https://openeo.vito.be/openeo/1.1", "-o", str(self.test_dir / "process_results.csv")],
                "description": "Check process compliance for single backend",
                "skip_if_missing": []
            },
            {
                "name": "Process compliance multiple backends",
                "cmd": ["python", "openeobench", "process", "-i", "backends.csv", "-o", str(self.test_dir / "process_compliance.csv")],
                "description": "Check process compliance for multiple backends",
                "skip_if_missing": ["backends.csv"]
            },
            
            # Process summary commands
            {
                "name": "Process summary (CSV)",
                "cmd": ["python", "openeobench", "process-summary", str(self.test_dir / "mock_process_results"), "--output", str(self.test_dir / "process_summary.csv"), "--format", "csv"],
                "description": "Generate process compliance summary (CSV)",
                "skip_if_missing": [],
                "setup_mock_data": True
            },
            {
                "name": "Process summary (MD)",
                "cmd": ["python", "openeobench", "process-summary", str(self.test_dir / "mock_process_results"), "--output", str(self.test_dir / "process_summary.md"), "--format", "md"],
                "description": "Generate process compliance summary (Markdown)",
                "skip_if_missing": [],
                "setup_mock_data": True
            },
            
            # Visualize commands
            {
                "name": "Visualize (help)",
                "cmd": ["python", "openeobench", "visualize", "--help"],
                "description": "Display visualize command help",
                "skip_if_missing": []
            },
            {
                "name": "Visualize folders with markdown format",
                "cmd": ["python", "openeobench", "visualize", str(self.test_dir / "mock_output1"), str(self.test_dir / "mock_output2"), "--output", str(self.test_dir / "visualization.md"), "--format", "md"],
                "description": "Create visual matrix from folders (Markdown)",
                "skip_if_missing": [],
                "setup_mock_data": True
            },
            {
                "name": "Visualize folders with PNG format",
                "cmd": ["python", "openeobench", "visualize", str(self.test_dir / "mock_output1"), str(self.test_dir / "mock_output2"), "--output", str(self.test_dir / "visualization.png"), "--format", "png"],
                "description": "Create visual matrix from folders (PNG)",
                "skip_if_missing": [],
                "setup_mock_data": True
            },
            {
                "name": "Visualize folders with both formats",
                "cmd": ["python", "openeobench", "visualize", str(self.test_dir / "mock_output1"), str(self.test_dir / "mock_output2"), "--output", str(self.test_dir / "visualization_both.md"), "--format", "both"],
                "description": "Create visual matrix from folders (both MD and PNG)",
                "skip_if_missing": [],
                "setup_mock_data": True
            }
        ]

    def setup_test_environment(self):
        """Setup test directories and mock data"""
        # Create test output directory
        self.test_dir.mkdir(exist_ok=True)
        logger.info(f"Created test directory: {self.test_dir}")
        
        # Create service results directory
        (self.test_dir / "service_results").mkdir(exist_ok=True)
        
        # Create run results directory
        (self.test_dir / "run_results").mkdir(exist_ok=True)

    def setup_mock_data_if_needed(self, command):
        """Setup mock data for commands that need it"""
        if not command.get("setup_mock_data", False):
            return
            
        # Create mock output directories with results.json files
        mock_dirs = ["mock_output1", "mock_output2", "mock_process_results"]
        for mock_dir in mock_dirs:
            dir_path = self.test_dir / mock_dir
            dir_path.mkdir(exist_ok=True)
            
            # Create mock results.json
            mock_results = {
                "job_id": "test-job-123",
                "status": "finished",
                "created": "2025-01-01T00:00:00Z",
                "started": "2025-01-01T00:01:00Z",
                "finished": "2025-01-01T00:05:00Z",
                "timing": {
                    "submit": 1.5,
                    "queue": 60.0,
                    "execution": 240.0,
                    "download": 30.0,
                    "total": 300.0
                }
            }
            
            with open(dir_path / "results.json", "w") as f:
                import json
                json.dump(mock_results, f, indent=2)
            
            # For process results, create mock CSV files
            if "process" in mock_dir:
                mock_csv_content = """process,level,status,compatibility,reason
load_collection,L1,available,compatible,Process available and compatible
save_result,L1,available,compatible,Process available and compatible
ndvi,L2,available,compatible,Process available and compatible
"""
                with open(dir_path / "backend1.csv", "w") as f:
                    f.write(mock_csv_content)
            
            # For visualization tests, create mock TIFF files
            # We'll create simple dummy files since we don't have GDAL for proper TIFF creation
            if mock_dir in ["mock_output1", "mock_output2"]:
                # Create a dummy file that looks like a TIFF (for file detection)
                mock_tiff_path = dir_path / f"result_{mock_dir}.tif"
                with open(mock_tiff_path, "wb") as f:
                    # Write minimal TIFF header (just enough for file detection)
                    f.write(b"II*\x00")  # TIFF magic number (little endian)
                    f.write(b"\x00" * 100)  # Pad with zeros

    def should_skip_command(self, command):
        """Check if a command should be skipped due to missing dependencies"""
        skip_files = command.get("skip_if_missing", [])
        
        for file_path in skip_files:
            path = Path(file_path)
            if not path.exists():
                # Check alternative files if specified
                alternative_files = command.get("alternative_files", [])
                found_alternative = False
                
                for alt_file in alternative_files:
                    if Path(alt_file).exists():
                        # Update command to use alternative file
                        cmd_str = " ".join(command["cmd"])
                        updated_cmd = cmd_str.replace(file_path, alt_file).split()
                        command["cmd"] = updated_cmd
                        found_alternative = True
                        logger.info(f"Using alternative file {alt_file} for command {command['name']}")
                        break
                
                if not found_alternative:
                    logger.warning(f"Skipping command '{command['name']}' - missing dependency: {file_path}")
                    return True
                    
        return False

    def run_command(self, command):
        """Execute a single command and log the results"""
        if self.should_skip_command(command):
            return {"status": "skipped", "reason": "Missing dependencies"}
        
        # Setup mock data if needed
        self.setup_mock_data_if_needed(command)
        
        logger.info(f"Testing command: {command['name']}")
        logger.info(f"Description: {command['description']}")
        logger.info(f"Command: {' '.join(command['cmd'])}")
        
        try:
            # Execute the command
            result = subprocess.run(
                command['cmd'],
                cwd=self.base_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            success = result.returncode == 0
            
            # Log the results
            if success:
                logger.info(f"✅ Command '{command['name']}' executed successfully")
                if result.stdout.strip():
                    logger.info(f"STDOUT:\n{result.stdout}")
            else:
                logger.error(f"❌ Command '{command['name']}' failed with return code {result.returncode}")
                if result.stderr.strip():
                    logger.error(f"STDERR:\n{result.stderr}")
                if result.stdout.strip():
                    logger.info(f"STDOUT:\n{result.stdout}")
            
            return {
                "status": "success" if success else "failed",
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr
            }
            
        except subprocess.TimeoutExpired:
            logger.error(f"⏰ Command '{command['name']}' timed out after 5 minutes")
            return {"status": "timeout", "reason": "Command timed out"}
            
        except Exception as e:
            logger.error(f"💥 Exception while executing command '{command['name']}': {str(e)}")
            return {"status": "exception", "reason": str(e)}

    def run_all_tests(self):
        """Run all command tests"""
        logger.info("=" * 80)
        logger.info("Starting OpenEO Bench Command Tests")
        logger.info("=" * 80)
        
        results = {}
        total_commands = len(self.commands)
        
        for i, command in enumerate(self.commands, 1):
            logger.info(f"\n[{i}/{total_commands}] Testing: {command['name']}")
            logger.info("-" * 60)
            
            result = self.run_command(command)
            results[command['name']] = result
            
            logger.info("-" * 60)
        
        # Generate summary
        self.generate_summary(results)
        
        return results

    def generate_summary(self, results):
        """Generate a summary of test results"""
        logger.info("\n" + "=" * 80)
        logger.info("TEST SUMMARY")
        logger.info("=" * 80)
        
        successful = sum(1 for r in results.values() if r.get("status") == "success")
        failed = sum(1 for r in results.values() if r.get("status") == "failed")
        skipped = sum(1 for r in results.values() if r.get("status") == "skipped")
        timeout = sum(1 for r in results.values() if r.get("status") == "timeout")
        exception = sum(1 for r in results.values() if r.get("status") == "exception")
        
        total = len(results)
        
        logger.info(f"Total commands tested: {total}")
        logger.info(f"✅ Successful: {successful}")
        logger.info(f"❌ Failed: {failed}")
        logger.info(f"⏩ Skipped: {skipped}")
        logger.info(f"⏰ Timeout: {timeout}")
        logger.info(f"💥 Exception: {exception}")
        logger.info(f"Success rate: {(successful/total)*100:.1f}%" if total > 0 else "N/A")
        
        # Detailed results
        logger.info(f"\nDetailed Results:")
        for name, result in results.items():
            status = result.get("status", "unknown")
            status_emoji = {
                "success": "✅",
                "failed": "❌",
                "skipped": "⏩",
                "timeout": "⏰",
                "exception": "💥"
            }.get(status, "❓")
            
            reason = result.get("reason", "")
            if reason:
                logger.info(f"{status_emoji} {name}: {status.upper()} - {reason}")
            else:
                logger.info(f"{status_emoji} {name}: {status.upper()}")
        
        logger.info(f"\nLog file saved to: {log_filename}")
        logger.info(f"Test output directory: {self.test_dir}")

if __name__ == "__main__":
    tester = CommandTester()
    results = tester.run_all_tests()
    
    # Exit with appropriate code
    failed_count = sum(1 for r in results.values() if r.get("status") in ["failed", "timeout", "exception"])
    sys.exit(1 if failed_count > 0 else 0)
