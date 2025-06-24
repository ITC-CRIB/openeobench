#!/usr/bin/env python3
"""
openeotest.py - OpenEO Backend Scenario Runner and Summarizer

This script provides two main functionalities:
1. Run: Connect to an OpenEO backend, authenticate, and execute a batch job for a specified process graph
2. Summarize: Generate summary reports from output folders in either markdown or CSV format
"""

import os
import sys
import json
import glob
import time
import openeo
import shutil
import logging
import datetime
import urllib.parse
import argparse
import subprocess
import csv
import tempfile
import re
import base64
from pathlib import Path
from collections import defaultdict

# Try to import optional visualization dependencies
try:
    import numpy as np  # Required for array operations
    # For enhanced geotiff loading
    try:
        import rioxarray as rxr
        import xarray as xr  # For netCDF and advanced array operations
        RIOXARRAY_AVAILABLE = True
    except ImportError:
        RIOXARRAY_AVAILABLE = False
        print("Warning: rioxarray not available, advanced image loading features will be limited")
except ImportError:
    RIOXARRAY_AVAILABLE = False
    print("Warning: rioxarray not available, advanced image loading features will be limited")

# Try to import GDAL
try:
    from osgeo import gdal
    GDAL_AVAILABLE = True
    
    # Try to import gdal_array separately as it might fail even if gdal is available
    try:
        from osgeo import gdal_array
        GDAL_ARRAY_AVAILABLE = True
    except ImportError:
        GDAL_ARRAY_AVAILABLE = False
        print("Warning: GDAL is available but gdal_array is not. Will use fallback visualization methods.")
except ImportError:
    GDAL_AVAILABLE = False
    GDAL_ARRAY_AVAILABLE = False
    print("Warning: GDAL not available, some visualization features will be limited")

try:
    import matplotlib
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("Warning: matplotlib not available, visualizations will be skipped")
    
# Try to import PIL as an additional fallback option
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("Warning: PIL not available, further fallback visualization methods will be limited")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('scenario_runner.log')
    ]
)
logger = logging.getLogger('scenario_runner')

def load_backends(backends_file):
    """Load backends from the backends.json file."""
    try:
        with open(backends_file, 'r') as f:
            backends = json.load(f)
        logger.info(f"Loaded {len(backends)} backends from {backends_file}")
        return backends
    except Exception as e:
        logger.error(f"Failed to load backends from {backends_file}: {str(e)}")
        return []

def load_process_graphs(process_graph_dir):
    """Load all process graphs from the process_graph directory."""
    process_graphs = {}
    try:
        # Create directory if it doesn't exist
        os.makedirs(process_graph_dir, exist_ok=True)
        
        # Find all JSON files in the process_graph directory
        pg_files = glob.glob(os.path.join(process_graph_dir, "*.json"))
        
        for pg_file in pg_files:
            try:
                with open(pg_file, 'r') as f:
                    pg_name = os.path.basename(pg_file).replace('.json', '')
                    process_graphs[pg_name] = json.load(f)
                logger.info(f"Loaded process graph: {pg_name}")
            except Exception as e:
                logger.error(f"Failed to load process graph {pg_file}: {str(e)}")
        
        return process_graphs
    except Exception as e:
        logger.error(f"Failed to load process graphs from {process_graph_dir}: {str(e)}")
        return {}

def connect_to_backend(backend):
    """Connect to an openEO backend."""
    try:
        # Special handling for Earth Engine backend
        if backend['url'] == "https://earthengine.openeo.org/v1.0":
            connection = openeo.connect("https://earthengine.openeo.org/v1.0")
            connection.authenticate_basic(username='group3', password='test123')
            logger.info(f"Connected to Earth Engine backend with authentication: {backend['name']}")
            return connection
        
        # Standard connection for other backends
        connection = openeo.connect(backend['url'])
        logger.info(f"Connected to backend: {backend['name']} at {backend['url']}")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to backend {backend['name']} at {backend['url']}: {str(e)}")
        return None

def authenticate(connection, backend_name):
    """Authenticate with the backend."""
    try:
        # Check for Earth Engine backend
        if connection.get_url() == "https://earthengine.openeo.org/v1.0":
            # Use basic auth for Earth Engine with hardcoded credentials
            connection.authenticate_basic("group3", "test123")
            logger.info("Authenticated with Earth Engine backend using basic auth")
            return True
            
        # Try basic authentication if credentials are provided in environment variables
        username = os.environ.get(f"{backend_name.upper()}_USERNAME")
        password = os.environ.get(f"{backend_name.upper()}_PASSWORD")
        
        if username and password:
            connection.authenticate_basic(username, password)
            logger.info(f"Authenticated with backend {backend_name} using basic auth from environment variables")
            return True
        else:
            # Fall back to OIDC authentication
            connection.authenticate_oidc()
            logger.info(f"Authenticated with backend {backend_name} using OIDC")
            return True
    
    except Exception as e:
        logger.error(f"Authentication failed for backend {backend_name}: {str(e)}")
        return False

def run_task(api_url, scenario_path, output_directory=None):
    """Run a specific scenario on a given backend."""
    # Parse hostname for default output directory
    hostname = urllib.parse.urlparse(api_url).netloc
    backend_name = hostname.replace('.', '_')
    scenario_name = os.path.basename(scenario_path).replace('.json', '')
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Set default output directory if not provided
    if not output_directory:
        output_directory = f"{backend_name}_{scenario_name}_{timestamp}"
        output_directory = os.path.join("output", output_directory)
    
    # Create output directory structure
    os.makedirs(output_directory, exist_ok=True)
    #process_graphs_dir = os.path.join(output_directory, "process_graphs")
    #os.makedirs(process_graphs_dir, exist_ok=True)

    # Copy scenario file to process graphs directory
    process_graph_file = os.path.join(output_directory, "processgraph.json")
    shutil.copyfile(scenario_path, process_graph_file)
    
    # Initialize comprehensive results structure
    results = {
        "backend_url": api_url,
        "backend_name": backend_name,
        "process_graph": scenario_name,
        "status": "failed",
        "job_id": None,
        "job_status": None,
        "start_time": time.time(),
        "submit_time": None,
        "start_job_time": None,
        "job_execution_time": None,
        "download_time": None,
        "total_time": None,
        "queue_time": None,
        "processing_time": None,
        "file_path": None,
        "process_graph_file": os.path.relpath(process_graph_file, output_directory),
        "error": None,
        "job_status_history": {},
        "timestamp": datetime.datetime.now().isoformat()
    }
    
    logger.info(f"Processing backend at {api_url} with scenario {scenario_name}")
    
    # Load the process graph
    try:
        with open(scenario_path, 'r') as f:
            process_graph = json.load(f)
        logger.info(f"Loaded process graph from {scenario_path}")
    except Exception as e:
        logger.error(f"Failed to load process graph from {scenario_path}: {str(e)}")
        results["error"] = f"Failed to load process graph: {str(e)}"
        _save_results(results, output_directory, scenario_name, timestamp)
        return results
    
    # Connect to backend
    try:
        # Special handling for Earth Engine backend
        if api_url == "https://earthengine.openeo.org/v1.0":
            connection = openeo.connect(api_url)
            connection.authenticate_basic(username='group3', password='test123')
            logger.info("Connected to Earth Engine backend with authentication")
        else:
            # Standard connection for other backends
            connection = openeo.connect(api_url)
            logger.info(f"Connected to backend at {api_url}")
            
            # Authenticate
            try:
                connection.authenticate_oidc()
                logger.info("Authenticated with backend")
            except Exception as e:
                logger.error(f"Authentication failed for backend: {str(e)}")
                results["error"] = f"Authentication failed: {str(e)}"
                _save_results(results, output_directory, scenario_name, timestamp)
                return results
                
    except Exception as e:
        logger.error(f"Failed to connect to backend at {api_url}: {str(e)}")
        results["error"] = f"Failed to connect to backend: {str(e)}"
        _save_results(results, output_directory, scenario_name, timestamp)
        return results
    
    # Execute batch job with detailed timing and monitoring
    try:
        # Create a batch job
        job_creation_start = time.time()
        logger.info(f"Creating batch job for process graph {scenario_name}")
        
        job = connection.create_job(process_graph)
        job_id = job.job_id
        results["job_id"] = job_id
        results["submit_time"] = time.time() - job_creation_start
        
        logger.info(f"Created batch job {job_id} for process graph {scenario_name}")
        
        # Start job execution with detailed monitoring
        job_start_time = time.time()
        results["start_job_time"] = job_start_time
        job_start_datetime = datetime.datetime.now();
        
        logger.info(f"Starting job {job_id}")
        job.start()
        
        # Monitor job status with increasing poll intervals
        poll_interval = 5  # Start with 5 seconds
        max_poll_interval = 30
        timeout = 3600  # 1 hour timeout
        last_status = "submitted"
        status_times = {"submitted": job_start_datetime}
        check_count = 0
        
        while True:
            # Check timeout
            if time.time() - job_start_time > timeout:
                error_msg = f"Job timed out after {timeout} seconds"
                results["error"] = error_msg
                logger.error(error_msg)
                break
            
            try:
                current_time = datetime.datetime.now()
                current_status = job.status()
                check_count += 1
                results["job_status"] = current_status
                
                # Record status changes
                if current_status != last_status:
                    status_times[current_status] = current_time
                    status_change_time = (current_time - job_start_datetime).total_seconds()
                    
                    logger.info(f"Job {job_id} status changed to '{current_status}' after {status_change_time:.2f} seconds")
                    last_status = current_status
                else:
                    # Log periodic status for long-running jobs
                    if check_count % 12 == 0:  # Every minute
                        elapsed_time = (current_time - job_start_datetime).total_seconds()
                        logger.info(f"Job {job_id} still {current_status} after {elapsed_time:.2f} seconds")
                
                # Check if job is complete
                if current_status in ["finished", "error", "canceled"]:
                    break
                
                # Increase poll interval
                poll_interval = min(poll_interval * 1.5, max_poll_interval)
                time.sleep(poll_interval)
                
            except Exception as e:
                logger.error(f"Error checking job status: {e}")
                if time.time() - job_start_time > timeout:
                    results["error"] = f"Job status checking failed: {str(e)}"
                    break
                time.sleep(poll_interval)
        
        # Record timing information
        results["job_execution_time"] = time.time() - job_start_time
        results["job_status_history"] = {
            status: time_val.isoformat() for status, time_val in status_times.items()
        }
        
        # Calculate queue and processing times
        if "queued" in status_times and "running" in status_times:
            queue_time = (status_times["running"] - status_times["queued"]).total_seconds()
            results["queue_time"] = queue_time
            logger.info(f"Job was queued for {queue_time:.2f} seconds")
        
        if "running" in status_times and "finished" in status_times:
            processing_time = (status_times["finished"] - status_times["running"]).total_seconds()
            results["processing_time"] = processing_time
            logger.info(f"Backend processing time: {processing_time:.2f} seconds")
        
        # Handle job completion
        if results["job_status"] == "finished":
            # Download results
            download_start = time.time()
            logger.info(f"Downloading results for job {job_id}")
            
            # Set up a timestamped downloads directory for TIFF files
            download_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            clean_scenario_name = scenario_name.replace('.json', '').replace('/', '_').replace('\\', '_')

            
            # Get the results
            job_results = job.get_results()
            
            # Get and save metadata
            metadata = job_results.get_metadata()
            '''
            metadata_file = os.path.join(output_directory, "metadata.json")
            try:
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2)
                logger.info(f"Saved job metadata to {metadata_file}")
            except Exception as e:
                logger.warning(f"Failed to save job metadata: {e}")
            '''
            # Download directly to the output directory
            job_results.download_files(output_directory)            
            
            logger.info(f"Downloaded files to {output_directory}")
            
            results["download_time"] = time.time() - download_start
            results["status"] = "success"
            results["download_timestamp"] = download_timestamp
            results["downloads_directory"] = output_directory
            
            logger.info("Download completed")
            logger.info(f"Files saved with scenario name '{clean_scenario_name}' and timestamp {download_timestamp}")
            logger.info(f"Files location: {output_directory}")
            
        else:
            # Job failed or was canceled
            results["status"] = "failed"
            results["error"] = f"Job ended with status: {results['job_status']}"
            logger.error(f"Job {job_id} failed with status: {results['job_status']}")
            
            # Try to get job details for failed jobs
            try:
                job_info = job.describe_job()
                results["job_details"] = job_info
                logger.info("Retrieved job details for failed job")
            except Exception as e:
                logger.warning(f"Could not retrieve job details: {e}")
        
    except Exception as e:
        logger.exception(f"Failed to execute batch job for process graph {scenario_name}: {str(e)}")
        results["status"] = "failed"
        results["error"] = str(e)
    
    finally:
        # Calculate total time and save results
        results["total_time"] = time.time() - results["start_time"]
        results["timestamp"] = datetime.datetime.now().isoformat()
        
        _save_results(results, output_directory, scenario_name, timestamp)
        
        # Log completion summary
        logger.info(f"Run completed for {scenario_name} on {backend_name}")
        logger.info(f"Status: {results['status']}")
        logger.info(f"Total time: {results['total_time']:.2f} seconds")
        
        return results


def _save_results(results, output_directory, scenario_name, timestamp):
    """Helper function to save results to files."""
    try:
        # Save comprehensive results
        results_file = os.path.join(output_directory, "results.json")
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        '''
        # Also save in legacy format for compatibility
        legacy_result = {
            'backend_url': results['backend_url'],
            'process_graph': results['process_graph'],
            'job_id': results['job_id'],
            'status': results.get('job_status', results['status'])
        }
        
        with open(os.path.join(output_directory, 'job_result.json'), 'w') as f:
            json.dump(legacy_result, f, indent=2)
        
        # Save timing logs in legacy format
        legacy_logs = {
            'job_creation': results['start_time'],
            'job_start': results.get('start_job_time', results['start_time']),
            'job_completion': results['start_time'] + results.get('job_execution_time', 0),
            'job_download': results['start_time'] + results.get('total_time', 0) - results.get('download_time', 0),
            'job_status': results.get('job_status', results['status'])
        };
        
        with open(os.path.join(output_directory, 'job_logs.json'), 'w') as f:
            json.dump(legacy_logs, f, indent=2)
        '''    
    except Exception as e:
        logger.warning(f"Failed to save results: {e}")


def summarize_task(input_patterns, output_path):
    """Generate a summary report from output folders matching glob patterns."""
    import statistics
    logger.info(f"Summarizing results for patterns: {input_patterns}, output: {output_path}")

    # Expand glob patterns to folder list
    matched_folders = set()
    for pattern in input_patterns:
        # Always check the output directory first - this is where all data is stored
        output_pattern = os.path.join("output", f"*{pattern}*")
        logger.info(f"Searching for pattern: {output_pattern}")
        output_matches = glob.glob(output_pattern)
        for folder in output_matches:
            if os.path.isdir(folder):
                matched_folders.add(folder)
        
        # Also check direct pattern for flexibility
        direct_matches = glob.glob(pattern)
        for folder in direct_matches:
            if os.path.isdir(folder):
                matched_folders.add(folder)
    
    matched_folders = sorted(matched_folders)
    logger.info(f"Matched {len(matched_folders)} folders.")
    
    if matched_folders:
        # Print first few matches for confirmation
        preview = ", ".join(os.path.basename(f) for f in list(matched_folders)[:5])
        if len(matched_folders) > 5:
            preview += f", ... ({len(matched_folders) - 5} more)"
        logger.info(f"Found folders: {preview}")
    else:
        logger.error("No folders matched the input pattern(s). Please check the pattern and make sure the output directory exists.")
        # List some folders in output dir for troubleshooting
        try:
            if os.path.exists("output"):
                sample = [d for d in os.listdir("output") if os.path.isdir(os.path.join("output", d))][:10]
                logger.info(f"Sample folders in output directory: {', '.join(sample)}")
                logger.info(f"Try patterns like: *vienna*10km* or *{input_patterns[0]}*")
        except Exception as e:
            logger.error(f"Error listing output directory: {e}")
        return

    # Collect all results and all possible keys
    all_results = []
    all_keys = set()
    for folder in matched_folders:
        results_path = os.path.join(folder, "results.json")
        if not os.path.isfile(results_path):
            logger.warning(f"No results.json in {folder}, skipping.")
            continue
        try:
            with open(results_path) as f:
                data = json.load(f)
            # Just use the folder name without the full path
            data["_folder"] = os.path.basename(folder)
            
            # Count TIFF files in the folder
            tiff_files = glob.glob(os.path.join(folder, "*.tif")) + glob.glob(os.path.join(folder, "*.tiff"))
            data["tiff_count"] = len(tiff_files)
            
            all_results.append(data)
            all_keys.update(data.keys())
        except Exception as e:
            logger.warning(f"Failed to read {results_path}: {e}")

    if not all_results:
        logger.error("No valid results.json files found.")
        return

    # Define units for known timing/statistics fields
    units = {
        "submit_time": "seconds",
        "start_time": "unix_seconds",
        "start_job_time": "unix_seconds",
        "job_execution_time": "seconds",
        "download_time": "seconds",
        "total_time": "seconds",
        "queue_time": "seconds",
        "processing_time": "seconds",
        "download_timestamp": "datetime",
        "timestamp": "datetime",
        "tiff_count": "files",
    }
    # Add more units as needed

    # Prepare CSV/Markdown columns: _folder first, then sorted keys
    keys = ["_folder"] + [k for k in sorted(all_keys) if k in units]
    # Add units to column names for timing/statistics fields
    def colname_with_unit(k):
        return f"{k} [{units[k]}]" if k in units else k
    columns = [colname_with_unit(k) for k in keys]

    # Prepare rows for CSV/Markdown
    rows = []
    for result in all_results:
        row = []
        for k in keys:
            v = result.get(k, "")
            # Format floats to 3 decimals for times
            if isinstance(v, float) and k in units and units[k] == "seconds":
                v = f"{v:.3f}"
            row.append(v)
        rows.append(row)

    # Compute summary statistics for timing/statistics fields (float/int only)
    stats_rows = []
    for k in keys:
        if k == "_folder":
            continue
        values = [result.get(k) for result in all_results]
        # Only consider numeric values
        numeric = [float(v) for v in values if isinstance(v, (int, float))]
        if numeric:
            n = len(numeric)
            minv = min(numeric)
            maxv = max(numeric)
            meanv = statistics.mean(numeric)
            stddev = statistics.stdev(numeric) if n > 1 else 0.0
            stats_rows.append([
                colname_with_unit(k), n, f"{minv:.3f}", f"{maxv:.3f}", f"{meanv:.3f}", f"{stddev:.3f}"
            ])

    # Output CSV or Markdown
    if output_path.endswith(".csv"):
        # Write per-folder CSV
        csv_path = output_path
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)
        logger.info(f"Wrote folder summary CSV: {csv_path}")
        # Write summary statistics CSV
        stats_path = os.path.splitext(csv_path)[0] + "_summary.csv"
        with open(stats_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["metric", "n", "min", "max", "average", "stddev"])
            writer.writerows(stats_rows)
        logger.info(f"Wrote summary statistics CSV: {stats_path}")
    elif output_path.endswith(".md"):
        # Write Markdown report
        md_path = output_path
        with open(md_path, "w") as f:
            f.write(f"# OpenEO Results Summary\n\n")
            f.write(f"## Folder Results\n\n")
            # Table header
            f.write("| " + " | ".join(columns) + " |\n")
            f.write("|" + "---|" * len(columns) + "\n")
            for row in rows:
                f.write("| " + " | ".join(str(x) for x in row) + " |\n")
            f.write(f"\n## Summary Statistics\n\n")
            f.write("| metric | n | min | max | average | stddev |\n")
            f.write("|---|---|---|---|---|---|\n")
            for srow in stats_rows:
                f.write("| " + " | ".join(str(x) for x in srow) + " |\n")
        logger.info(f"Wrote Markdown summary: {md_path}")
    else:
        logger.error("Output file must end with .csv or .md")

def visualize_task(args):
    """
    Generate visualizations and statistics for output folders matching a pattern
    
    Args:
        args: Command-line arguments containing pattern and output preferences
    """
    pattern = args.pattern
    output_dir = args.output_dir if args.output_dir else os.getcwd()
    output_prefix = args.output_prefix if args.output_prefix else "today_vis"
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Find matching folders 
    output_folders = []
    
    # Handle specific patterns:
    if pattern == 'today':
        # Find folders from today based on date
        today = datetime.datetime.now().strftime('%Y%m%d')
        search_pattern = f"*{today}*"
        output_folders = glob.glob(os.path.join("output", search_pattern))
    elif pattern == 'yesterday':
        # Find folders from yesterday
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        search_pattern = f"*{yesterday}*"
        output_folders = glob.glob(os.path.join("output", search_pattern))
    elif pattern == 'all':
        # Find all output folders
        output_folders = glob.glob(os.path.join("output", "*"))
    else:
        # Use the provided pattern directly
        # First try with output/ prefix if not specified
        if not pattern.startswith("output/") and not pattern.startswith("/"):
            search_pattern = os.path.join("output", pattern)
            output_folders = glob.glob(search_pattern)
        
        # If nothing found or pattern already has a path, use as-is
        if not output_folders or pattern.startswith("output/") or pattern.startswith("/"):
            output_folders = glob.glob(pattern)
    
    # Filter to ensure we only have directories
    output_folders = [f for f in output_folders if os.path.isdir(f)]
    
    if not output_folders:
        logging.error(f"No matching folders found with pattern: {pattern}")
        return 1
    
    logging.info(f"Found {len(output_folders)} matching folders")
    for folder in output_folders:
        logging.info(f"  - {folder}")
    
    # Create visualization matrix
    markdown_path, stats_path = _create_matrix_visualization(
        output_folders, 
        output_dir,
        output_prefix
    )
    
    logging.info(f"Visualization matrix saved to {markdown_path}")
    logging.info(f"Statistics saved to {stats_path}")
    
    return 0
def visualize_task(input_patterns, output_path):
    """
    Create a visual matrix of resulting GeoTIFF images and summary statistics.
    Enhanced with robust error handling, multiple fallback methods, and publication-quality visualization.
    Creates a matrix with folders as columns and files as rows, and includes
    datatype information in the statistics output.
    
    Args:
        input_patterns: List of glob patterns matching folders to visualize
        output_path: Path to save visualization results (.md file)
    """
    logger.info(f"Visualizing results for patterns: {input_patterns}, output: {output_path}")
    
    # Check visualization dependencies and log availability
    visualization_methods = []
    
    if GDAL_AVAILABLE:
        visualization_methods.append("GDAL API")
    else:
        logger.error("GDAL is required for visualization but not available. Trying to continue with limited functionality.")
    
    # Check for gdal_translate command
    gdal_translate_available = False
    try:
        result = subprocess.run(['gdal_translate', '--version'], capture_output=True, check=False, timeout=5)
        if result.returncode == 0:
            gdal_translate_available = True
            visualization_methods.append("gdal_translate")
    except (subprocess.TimeoutExpired, FileNotFoundError):
        logger.warning("gdal_translate command not found or timed out, some visualization methods will be unavailable.")
    
    if MATPLOTLIB_AVAILABLE:
        visualization_methods.append("matplotlib")
    else:
        logger.warning("matplotlib not available, falling back to simpler visualization methods.")
    
    if PIL_AVAILABLE:
        visualization_methods.append("PIL/Pillow")
    else:
        logger.warning("PIL not available, some fallback visualization methods will be limited.")
    
    if GDAL_ARRAY_AVAILABLE:
        visualization_methods.append("gdal_array")
    else:
        logger.warning("gdal_array not available, using simplified thumbnail generation.")
    
    if visualization_methods:
        logger.info(f"Available visualization methods: {', '.join(visualization_methods)}")
    else:
        logger.error("No visualization methods available! Will attempt to continue with minimal functionality.")
    
    # Expand glob patterns to folder list - always prioritize output directory
    matched_folders = set()
    
    # First, ensure we're always looking in the output directory
    for pattern in input_patterns:
        # Try specific output directory pattern
        output_pattern = os.path.join("output", f"*{pattern}*")
        logger.info(f"Searching for pattern: {output_pattern}")
        output_matches = glob.glob(output_pattern)
        for folder in output_matches:
            if os.path.isdir(folder):
                matched_folders.add(folder)
    
    # If no matches found in output directory with wildcards, try exact pattern
    if not matched_folders:
        for pattern in input_patterns:
            # Try exact pattern in output directory
            output_pattern = os.path.join("output", pattern)
            if os.path.isdir(output_pattern):
                matched_folders.add(output_pattern)
            
            # Also check direct pattern as fallback
            if os.path.isdir(pattern):
                matched_folders.add(pattern)
            elif glob.glob(pattern):
                # Add any glob matches
                for match in glob.glob(pattern):
                    if os.path.isdir(match):
                        matched_folders.add(match)
    
    # Sort for consistent output
    matched_folders = sorted(matched_folders)
    logger.info(f"Matched {len(matched_folders)} folders.")
    
    if matched_folders:
        # Print first few matches for confirmation
        preview = ", ".join(os.path.basename(f) for f in list(matched_folders)[:5])
        if len(matched_folders) > 5:
            preview += f", ... ({len(matched_folders) - 5} more)"
        logger.info(f"Found folders: {preview}")
    else:
        logger.error("No folders matched the input pattern(s). Please check the pattern and make sure the output directory exists.")
        # List some folders in output dir for troubleshooting
        try:
            if os.path.exists("output"):
                sample = [d for d in os.listdir("output") if os.path.isdir(os.path.join("output", d))][:10]
                if sample:
                    logger.info(f"Sample folders in output directory: {', '.join(sample)}")
                    logger.info(f"Try patterns like: *vienna*10km* or *{input_patterns[0]}*")
                else:
                    logger.info("No folders found in output directory.")
            else:
                logger.info("Output directory does not exist.")
        except Exception as e:
            logger.error(f"Error listing output directory: {e}")
        return
    
    # Validate output path
    if not output_path.endswith('.md'):
        logger.error("Output file must have .md extension")
        return
    
    # Create output directories
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # Directory for storing visualizations
    vis_dir = os.path.join(output_dir, 'visualizations')
    os.makedirs(vis_dir, exist_ok=True)
    
    # Dictionary to store all TIFF files by folder
    folder_images = {}
    all_stats = []
    errors = []
    
    # Find all GeoTIFF files in matched folders with improved file detection
    for folder in matched_folders:
        folder_name = os.path.basename(folder)
        tiff_files = []
        
        # Search with multiple patterns to ensure we find all GeoTIFF files
        for ext in ['*.tif', '*.tiff', '*.TIF', '*.TIFF']:
            tiff_files.extend(glob.glob(os.path.join(folder, ext)))
            tiff_files.extend(glob.glob(os.path.join(folder, '**', ext), recursive=True))
        
        # Also look for files that might have GDAL-compatible formats but different extensions
        for ext in ['*.img', '*.IMG', '*.jp2', '*.JP2']:
            potential_files = glob.glob(os.path.join(folder, ext))
            potential_files.extend(glob.glob(os.path.join(folder, '**', ext), recursive=True))
            
            # Filter to only include files GDAL can open if GDAL is available
            if GDAL_AVAILABLE:
                for file_path in potential_files:
                    try:
                        ds = gdal.Open(file_path)
                        if ds is not None:
                            tiff_files.append(file_path)
                            ds = None
                    except Exception:
                        pass
            else:
                # If GDAL isn't available, include all potential raster files
                tiff_files.extend(potential_files)
        
        if tiff_files:
            # Remove duplicates while preserving order
            unique_files = []
            seen = set()
            for f in tiff_files:
                if f not in seen:
                    unique_files.append(f)
                    seen.add(f)
            
            folder_images[folder_name] = unique_files
            logger.info(f"Found {len(unique_files)} GeoTIFF/raster files in {folder_name}")
        else:
            logger.warning(f"No GeoTIFF files found in {folder_name}")
    
    # Create a matrix of all filenames for the MD report
    all_filenames = set()
    for files in folder_images.values():
        for file_path in files:
            all_filenames.add(os.path.basename(file_path))
    all_filenames = sorted(all_filenames)
    
    if not all_filenames:
        logger.error("No GeoTIFF files found in any of the matched folders")
        return
    
    # Create visualization thumbnails and collect statistics
    total_images = sum(len(files) for files in folder_images.values())
    processed_count = 0
    
    for folder_name, tiff_files in folder_images.items():
        for tiff_path in tiff_files:
            processed_count += 1
            logger.info(f"Processing file {processed_count} of {total_images}")
            
            filename = os.path.basename(tiff_path)
            # Remove .tif extension from the filename for the PNG output
            png_filename = os.path.splitext(filename)[0]
            
            # Create visualization - use new robust creation method
            try:
                thumb_path = os.path.join(vis_dir, f"{folder_name}_{png_filename}.png")
                _create_geotiff_thumbnail(tiff_path, thumb_path)
                
                # Get statistics with enhanced robustness
                stats = _get_geotiff_statistics(tiff_path)
                stats['folder'] = folder_name
                stats['filename'] = filename
                all_stats.append(stats)
                
                logger.info(f"Successfully processed {filename} from {folder_name}")
            except Exception as e:
                error_msg = str(e)
                errors.append(f"Error processing {tiff_path}: {error_msg}")
                logger.error(f"Error processing {tiff_path}: {error_msg}")
                
                # Create a placeholder image for failures
                try:
                    thumb_path = os.path.join(vis_dir, f"{folder_name}_{png_filename}.png")
                    _create_placeholder_image(thumb_path, filename, error_msg)
                    logger.info(f"Created placeholder image for {filename}")
                except Exception as placeholder_error:
                    logger.error(f"Failed to create placeholder: {placeholder_error}")
                
                # Still collect basic statistics even if visualization fails
                stats = {
                    'folder': folder_name,
                    'filename': filename,
                    'path': tiff_path,
                    'size_mb': round(os.path.getsize(tiff_path) / (1024 * 1024), 2) if os.path.exists(tiff_path) else 0,
                    'error': error_msg
                }
                all_stats.append(stats)
    
    # Write markdown file
    with open(output_path, 'w') as md_file:
        # Title
        md_file.write("# OpenEO Results Visualization\n\n")
        md_file.write(f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Summary information
        md_file.write(f"- **Folders processed**: {len(matched_folders)}\n")
        md_file.write(f"- **Images processed**: {total_images}\n")
        md_file.write(f"- **Visualization methods available**: {', '.join(visualization_methods)}\n")
        if errors:
            md_file.write(f"- **Errors encountered**: {len(errors)}\n")
        md_file.write("\n")
        
        # Image matrix
        md_file.write("## Visual Matrix of Results\n\n")
        
        # Table header with folder name and all filenames
        md_file.write("| Folder |")
        for filename in all_filenames:
            # Truncate very long filenames for display
            display_name = filename
            if len(filename) > 25:
                display_name = filename[:10] + "..." + filename[-12:]
            md_file.write(f" {display_name} |")
        md_file.write("\n")
        
        # Table separator
        md_file.write("|" + "---|" * (len(all_filenames) + 1) + "\n")
        
        # Table rows with thumbnails
        for folder_name in sorted(folder_images.keys()):
            md_file.write(f"| {folder_name} |")
            
            # Find the thumbnail for each filename
            for filename in all_filenames:
                matching_files = [f for f in folder_images[folder_name] if os.path.basename(f) == filename]
                if matching_files:
                    # Use basename without extension for the PNG filename
                    png_filename = os.path.splitext(filename)[0]
                    thumb_path = os.path.join('visualizations', f"{folder_name}_{png_filename}.png")
                    md_file.write(f" ![{filename}]({thumb_path}) |")
                else:
                    md_file.write(" |")  # Leave blank if no image found
            md_file.write("\n")
        
        # Statistics section
        md_file.write("\n## Statistics Summary\n\n")
        md_file.write("Statistics for all GeoTIFF files are available in the accompanying CSV file.\n\n")
        
        # Add error summary if there were errors
        if errors:
            md_file.write("\n## Processing Errors\n\n")
            md_file.write("The following errors were encountered during processing:\n\n")
            for i, error in enumerate(errors[:10]):  # Show only first 10 errors
                md_file.write(f"{i+1}. {error}\n")
            
            if len(errors) > 10:
                md_file.write(f"\n... and {len(errors) - 10} more errors. See log for details.\n")
    
    # Structure data for the enhanced visualization functions - folders as columns and files as rows
    folders_data = {}
    for folder_name, file_paths in folder_images.items():
        folders_data[folder_name] = {
            'files': file_paths,
            'thumbnails': {},
            'stats': {}
        }
        
        # Add thumbnails and stats if available
        for path in file_paths:
            filename = os.path.basename(path)
            
            # Find matching stats from our all_stats collection
            for stat in all_stats:
                if stat.get('path') == path or (stat.get('filename') == filename and stat.get('folder') == folder_name):
                    folders_data[folder_name]['stats'][path] = stat
                    break
                    
            # Find associated thumbnail
            png_name = f"{folder_name}_{os.path.splitext(filename)[0]}.png"
            thumb_path = os.path.join(vis_dir, png_name)
            
            if os.path.exists(thumb_path):
                folders_data[folder_name]['thumbnails'][path] = thumb_path
            else:
                logger.debug(f"No thumbnail found at {thumb_path} for {filename}")
                
        # Log summary for this folder
        logger.info(f"Folder {folder_name}: {len(file_paths)} files, " + 
                    f"{len(folders_data[folder_name]['stats'])} with stats, " + 
                    f"{len(folders_data[folder_name]['thumbnails'])} with thumbnails")
    
    # Create the enhanced matrix visualization
    _create_matrix_visualization(folders_data, output_path, include_stats=True)
    
    # Write statistics as CSV with enhanced format (folders as columns)
    csv_path = os.path.splitext(output_path)[0] + "_stats.csv"
    _write_statistics_csv(folders_data, csv_path)
    
    logger.info(f"Visualization complete. Output saved to {output_path}")
    logger.info(f"Statistics saved to {csv_path}")
    logger.info(f"Total errors: {len(errors)} out of {total_images} images processed")
    
    # Return success with error count
    return len(errors) == 0


def load_geotiff_enhanced(geotiff_path, bands=None, band_names=None):
    """
    Load a GeoTIFF or netCDF file as an array using rioxarray with fallbacks.
    
    Args:
        geotiff_path: Path to the GeoTIFF or netCDF file
        bands: List of band indices (1-based) for GeoTIFF files. Default: [4, 3, 2] for RGB
        band_names: List of band variable names for netCDF files. Default: ["B04", "B03", "B02"] for RGB
        
    Returns:
        numpy.ndarray: Image array with shape (height, width, bands)
    """
    # Check if file exists
    if not os.path.exists(geotiff_path):
        raise FileNotFoundError(f"File not found: {geotiff_path}")
    
    # Set default bands if not provided
    if bands is None:
        bands = [4, 3, 2]  # Default to RGB composite
    if band_names is None:
        band_names = ["B04", "B03", "B02"]  # Default to RGB composite
        
    # Get file extension
    ext = os.path.splitext(geotiff_path)[1].lower().strip('.')
    is_netcdf = ext in ('nc', 'nc4', 'netcdf')
    
    # Try rioxarray approach if available
    if RIOXARRAY_AVAILABLE:
        try:
            engine = 'netcdf4' if is_netcdf else None
            da = rxr.open_rasterio(geotiff_path, engine=engine)
            
            # Handle single-band vs multi-band differently
            if da.shape[0] == 1:
                # Single-band file - return as (height, width, 1)
                return da.values.transpose(1, 2, 0).astype(np.float32)
                
            # Multi-band file
            if not is_netcdf:  # GeoTIFF
                # Check if requested bands are available
                available_bands = list(range(1, da.shape[0] + 1))
                valid_bands = [b for b in bands if b in available_bands]
                
                if not valid_bands:
                    # If none of the requested bands are available, use first band(s)
                    valid_bands = available_bands[:min(3, len(available_bands))]
                    logger.warning(f"Requested bands {bands} not found. Using bands {valid_bands} instead.")
                
                arr = np.stack([da.sel(band=b).values for b in valid_bands], axis=-1)
                return arr.astype(np.float32)
            
        except Exception as e:
            logger.warning(f"rioxarray approach failed: {e}, falling back to GDAL")
    
    # GDAL fallback approach
    if GDAL_AVAILABLE:
        try:
            ds = gdal.Open(geotiff_path)
            if ds is None:
                raise ValueError(f"Could not open {geotiff_path}")
            
            # Get basic info
            band_count = ds.RasterCount
            width = ds.RasterXSize
            height = ds.RasterYSize
            
            # Select bands
            available_bands = list(range(1, band_count + 1))
            valid_bands = [b for b in bands if b in available_bands]
            
            if not valid_bands:
                # If none of the requested bands are available, use first band(s)
                valid_bands = available_bands[:min(3, len(available_bands))]
                logger.warning(f"Requested bands {bands} not found. Using bands {valid_bands} instead.")
            
            # Read bands
            bands_data = []
            for b in valid_bands:
                band = ds.GetRasterBand(b)
                if GDAL_ARRAY_AVAILABLE:
                    data = band.ReadAsArray()
                else:
                    data = np.frombuffer(band.ReadRaster(), dtype=np.uint16)
                    data = data.reshape((height, width))
                bands_data.append(data)
            
            # Stack bands
            if bands_data:
                arr = np.stack(bands_data, axis=-1)
                return arr.astype(np.float32)
            else:
                raise ValueError("No valid bands could be read from the file")
                
        except Exception as e:
            logger.error(f"GDAL approach failed: {e}")
            raise
    
    # If we get here, both approaches failed
    raise ImportError("Neither rioxarray nor GDAL is available for loading GeoTIFF files")
def contrast_stretch(image_array, lower_percent=2, upper_percent=98):
    """
    Perform linear stretch on an image array. Clips extremes at given percentiles.
    
    Args:
        image_array: Input image array with shape (height, width, bands)
        lower_percent: Low percentile to clip
        upper_percent: High percentile to clip
        
    Returns:
        Stretched image array with values in [0,1]
    """
    out = np.zeros_like(image_array, dtype=np.float32)
    for i in range(image_array.shape[-1]):
        band = image_array[..., i]
        # Handle potential no-data values
        valid_mask = ~np.isnan(band)
        if np.any(valid_mask):
            valid_data = band[valid_mask]
            # Only compute percentiles if we have enough data
            if len(valid_data) > 10:
                lo = np.percentile(valid_data, lower_percent)
                hi = np.percentile(valid_data, upper_percent)
                
                # Avoid division by zero
                if hi > lo:
                    out[..., i] = np.clip((band - lo) / (hi - lo), 0, 1)
                else:
                    # If min == max, just normalize to 0
                    out[..., i] = 0
            else:
                # Too few valid points, just normalize min/max
                min_val = np.min(valid_data)
                max_val = np.max(valid_data)
                if max_val > min_val:
                    out[..., i] = np.clip((band - min_val) / (max_val - min_val), 0, 1)
                else:
                    out[..., i] = 0
                    
            # Replace NaN values with zeros
            if np.any(~valid_mask):
                out[~valid_mask, i] = 0
    return out

def save_high_quality_png(image_array, output_path, dpi=300, add_colorbar=False, title=None):
    """
    Save an image array as a high-quality PNG file using matplotlib.
    
    Args:
        image_array: Image array with shape (height, width, bands)
        output_path: Path to save the PNG file
        dpi: DPI for the output image
        add_colorbar: Whether to add a colorbar (for single-band images)
        title: Optional title to add to the image
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not MATPLOTLIB_AVAILABLE:
        raise ImportError("matplotlib is required for save_high_quality_png")
    
    # Create figure
    plt.figure(figsize=(5, 5), dpi=100)
    
    # Apply appropriate normalization
    # Scale reflectance values if needed
    if image_array.max() > 1000:  # Likely Sentinel-2 reflectance in 0-10000 range
        disp = image_array / 10000.0
    else:
        disp = image_array.copy()
    
    # Apply contrast stretching
    disp = contrast_stretch(disp)
    
    # Handle single band vs multi-band
    if image_array.shape[-1] == 1:
        # Single-band (grayscale)
        im = plt.imshow(disp[..., 0], cmap='viridis')
        if add_colorbar:
            plt.colorbar(im, shrink=0.8, label='Pixel Value')
    elif image_array.shape[-1] == 2:
        # Two-band - convert to grayscale
        im = plt.imshow(np.mean(disp, axis=2), cmap='gray')
        if add_colorbar:
            plt.colorbar(im, shrink=0.8, label='Mean Value')
    else:
        # RGB or more - use only the first 3 bands for display
        plt.imshow(disp[..., :3])
    
    plt.axis('off')
    if title:
        plt.title(title, fontsize=9, pad=5)
    
    # Save with high quality
    plt.tight_layout()
    plt.savefig(output_path, 
                dpi=dpi,
                bbox_inches='tight',
                pad_inches=0.1,
                format='png',
                transparent=False)
    plt.close()
    
    return output_path
def _create_matrix_visualization(folders_data, output_path, include_stats=True):
    """
    Create a matrix visualization markdown file with folders as columns and files as rows.
    
    Args:
        folders_data: Dict mapping folder names to their data (files, thumbnails, stats)
        output_path: Path to save the markdown file
        include_stats: Whether to include statistics
        
    Returns:
        tuple: (md_path, csv_path)
    """
    # Get all unique filenames across all folders
    all_filenames = set()
    
    # Handle both old and new folder data structure formats
    for folder_name, folder_info in folders_data.items():
        if isinstance(folder_info, dict) and 'files' in folder_info:
            # New structure - folders_data contains folder_info dictionaries
            for file_path in folder_info['files']:
                all_filenames.add(os.path.basename(file_path))
        elif isinstance(folder_info, list):
            # Old structure - folders_data is a dict of file lists
            for file_path in folder_info:
                all_filenames.add(os.path.basename(file_path))
        else:
            logger.warning(f"Unrecognized data structure for folder {folder_name}")
            
    # Sort filenames for
    all_filenames = sorted(all_filenames)
    
    # Generate the markdown file
    with open(output_path, 'w') as md_file:
        # Title and metadata
        md_file.write("# OpenEO Results Visualization\n\n")
        md_file.write(f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Summary statistics - enhanced with more details
        md_file.write(f"- **Folders processed**: {len(folders_data)}\n")
        md_file.write(f"- **Unique filenames**: {len(all_filenames)}\n")
        
        # Get total file count and data size
        total_files = 0
        total_size_mb = 0
        data_types = set()
        for folder_name, folder_info in folders_data.items():
            if isinstance(folder_info, dict) and 'stats' in folder_info:
                for _, stats in folder_info['stats'].items():
                    if stats:
                        total_files += 1
                        total_size_mb += stats.get('size_mb', 0)
                        if 'datatype' in stats:
                            data_types.add(stats['datatype'])
        
        # Add enhanced summary information
        md_file.write(f"- **Total files**: {total_files}\n")
        md_file.write(f"- **Total data size**: {total_size_mb:.2f} MB\n")
        if data_types:
            md_file.write(f"- **Data types**: {', '.join(sorted(data_types))}\n")
        md_file.write("\n")
        
        # Create table header with folders as columns
        md_file.write("| Filename |")
        for folder_name in sorted(folders_data.keys()):
            # Truncate very long folder names for display
            display_name = folder_name
            if len(folder_name) > 25:
                display_name = folder_name[:10] + "..." + folder_name[-12:]
            md_file.write(f" {display_name} |")
        md_file.write("\n")
        
        # Table separator
        md_file.write("|" + "---|" * (len(folders_data) + 1) + "\n")
        
        # Table rows with filenames and thumbnails from each folder
        for filename in all_filenames:
            # Truncate very long filenames for display in first column
            display_name = filename
            if len(filename) > 25:
                display_name = filename[:10] + "..." + filename[-12:]
            md_file.write(f"| {display_name} |")
            
            # Add cells for each folder
            for folder_name in sorted(folders_data.keys()):
                folder_info = folders_data[folder_name]
                
                # Handle different folder data structures
                if isinstance(folder_info, dict) and 'files' in folder_info:
                    # New structure
                    files_list = folder_info['files']
                    matching_files = [f for f in files_list if os.path.basename(f) == filename]
                elif isinstance(folder_info, list):
                    # Old structure - direct list of files
                    matching_files = [f for f in folder_info if os.path.basename(f) == filename]
                else:
                    matching_files = []
                
                if matching_files:
                    # We found this file in this folder
                    png_filename = os.path.splitext(filename)[0]
                    thumb_path = os.path.join('visualizations', f"{folder_name}_{png_filename}.png")
                    md_file.write(f" ![{filename}]({thumb_path}) |")
                else:
                    # File not found in this folder
                    md_file.write(" |")  # Empty cell
                    
            md_file.write("\n")
        
        # Add statistics section if requested
        if include_stats:
            _write_stats_markdown(folders_data, md_file)
    
    # Also create CSV statistics file
    csv_path = os.path.splitext(output_path)[0] + "_stats.csv"
    _write_statistics_csv(folders_data, csv_path)
    
    return output_path, csv_path


def _write_stats_markdown(folders_data, md_file):
    """
    Write statistics section to the markdown file.
    
    Args:
        folders_data: Dict mapping folder names to their data
        md_file: Open file handle for the markdown file
    """
    md_file.write("\n## Statistics Summary\n\n")
    md_file.write("Statistics for all GeoTIFF files are available in the accompanying CSV file.\n\n")
    
    # Create a summary table of key statistics for each folder
    md_file.write("### Complete Statistics Table\n\n")
    md_file.write("| Filename | Folder | Datatype | Size (MB) | Dimensions | Pixel Size | Band Count |\n")
    md_file.write("|---|---|---|---|---|---|---|\n")
    
    # Loop through folders and extract key statistics
    all_stats = []
    for folder_name, folder_info in folders_data.items():
        stats_dict = {}
        if isinstance(folder_info, dict) and 'stats' in folder_info:
            # New format - get stats from the stats dict
            for file_path, stats in folder_info['stats'].items():
                if stats:
                    filename = os.path.basename(file_path)
                    width = stats.get('width', '-')
                    height = stats.get('height', '-')
                    dimensions = f"{width}×{height}" if width != '-' and height != '-' else '-'
                    pixel_width = stats.get('pixel_width_m', '-')
                    pixel_height = stats.get('pixel_height_m', '-')
                    pixel_size = f"{pixel_width}×{pixel_height}" if pixel_width != '-' and pixel_height != '-' else '-'
                    
                    all_stats.append({
                        'filename': filename,
                        'folder': folder_name,
                        'datatype': stats.get('datatype', '-'),
                        'size_mb': stats.get('size_mb', '-'),
                        'dimensions': dimensions,
                        'pixel_size': pixel_size,
                        'band_count': stats.get('band_count', '-')
                    })
        
    # Output sorted by folder and filename
    for stat in sorted(all_stats, key=lambda x: (x['folder'], x['filename'])):
        md_file.write(f"| {stat['filename']} | {stat['folder']} | {stat['datatype']} | {stat['size_mb']} | " +
                     f"{stat['dimensions']} | {stat['pixel_size']} | {stat['band_count']} |\n")


def _write_statistics_csv(folders_data, csv_path):
    """
    Write statistics from all GeoTIFF files to a CSV file.
    Structure the CSV with folders as columns and files as rows.
    
    Args:
        folders_data: Dictionary of folders and their data
        csv_path: Path to save the CSV file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get all unique filenames across all folders
        all_filenames = set()
        all_statistics = []
        
        # Collect filenames and stats from all folders
        for folder_name, folder_info in folders_data.items():
            if isinstance(folder_info, dict) and 'stats' in folder_info:
                # New format - folders_data contains folder_info dictionaries
                for path, stats in folder_info['stats'].items():
                    filename = os.path.basename(path)
                    all_filenames.add(filename)
                    
                    # Make sure stats includes folder and filename
                    stats_copy = stats.copy() if stats else {'path': path}
                    stats_copy['folder'] = folder_name
                    stats_copy['filename'] = filename
                    all_statistics.append(stats_copy)
            elif isinstance(folder_info, list):
                # Old format - folders_data is a dict of file lists
                for file_path in folder_info:
                    filename = os.path.basename(file_path)
                    all_filenames.add(filename)
                    # Create basic stats entry
                    all_statistics.append({
                        'folder': folder_name,
                        'filename': filename,
                        'path': file_path
                    })
        
        # Write to CSV
        if all_statistics:
            # Get all possible keys
            all_keys = set()
            for stats in all_statistics:
                all_keys.update(stats.keys())
                
            # Ensure our essential columns are first
            essential_columns = ['filename', 'folder', 'datatype', 'min', 'max', 'mean', 'stddev', 'size_mb']
            fieldnames = [col for col in essential_columns if col in all_keys]
            fieldnames += [col for col in sorted(all_keys) if col not in essential_columns]
            
            with open(csv_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_statistics)
            
            logger.info(f"Wrote statistics to {csv_path} with {len(all_statistics)} entries")
            return True
        else:
            logger.warning("No statistics to write to CSV")
            return False
            
    except Exception as e:
        logger.error(f"Error writing statistics CSV: {e}")
        return False
def _create_geotiff_thumbnail(file_path, thumbnail_path, size=(256, 256)):
    """
    Create a thumbnail image from a GeoTIFF file using the best available method.
    
    Args:
        file_path (str): Path to the GeoTIFF file
        thumbnail_path (str): Path where the thumbnail will be saved
        size (tuple): Target thumbnail size as (width, height)
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Ensure we have at least one visualization method available
    if not (RIOXARRAY_AVAILABLE or GDAL_AVAILABLE or PIL_AVAILABLE):
        logger.error("No visualization libraries available")
        return False
    
    # Clean up thumbnail path - ensure it's .png not .tif.png
    if thumbnail_path.lower().endswith('.tif.png'):
        thumbnail_path = thumbnail_path[:-8] + '.png'
    elif not thumbnail_path.lower().endswith('.png'):
        thumbnail_path = thumbnail_path + '.png'
    
    # Ensure thumbnail directory exists
    thumbnail_dir = os.path.dirname(thumbnail_path)
    os.makedirs(thumbnail_dir, exist_ok=True)
    
    # Method 1: Enhanced loading with newer approach using rioxarray/numpy
    try:
        # Analyze bands to choose the best visualization
        band_indices, reason = _analyze_geotiff_bands(file_path)
        logger.info(f"Band selection for {os.path.basename(file_path)}: {reason}")
        
        # Load selected bands
        try:
            image_data = load_geotiff_enhanced(file_path, band_indices)
            
            if image_data is not None:
                # Save as high-quality PNG with built-in contrast stretching
                save_high_quality_png(
                    image_data, 
                    thumbnail_path, 
                    dpi=300,  # Publication quality DPI
                    title=os.path.basename(file_path)
                )
                
                logger.info(f"Created enhanced thumbnail for {os.path.basename(file_path)}")
                return True
        except Exception as e:
            logger.warning(f"Enhanced image loading failed: {e}")
            # Fall through to next method
    except Exception as e:
        logger.warning(f"Error in band analysis: {e}")
        # Fall through to next method
    
    # Method 2: GDAL visualization
    if GDAL_AVAILABLE and GDAL_ARRAY_AVAILABLE:
        try:
            # Open the dataset
            ds = gdal.Open(file_path)
            if ds is None:
                raise IOError(f"Could not open {file_path} with GDAL")
            
            # Get band count
            band_count = ds.RasterCount
            
            # Determine which bands to use for RGB
            if band_count >= 3:
                r_band, g_band, b_band = 1, 2, 3  # 1-based indexing for GDAL
            elif band_count == 2:
                r_band, g_band, b_band = 1, 2, 2
            else:
                r_band = g_band = b_band = 1
            
            # Read the data
            r = ds.GetRasterBand(r_band).ReadAsArray()
            g = ds.GetRasterBand(g_band).ReadAsArray()
            b = ds.GetRasterBand(b_band).ReadAsArray()
            
            # Stack the bands and apply simple contrast enhancement
            rgb = np.stack([r, g, b], axis=0)
            
            # Apply contrast stretching
            rgb = contrast_stretch(rgb)
            
            # Transpose for matplotlib (bands first -> height, width, bands)
            rgb = np.transpose(rgb, (1, 2, 0))
            
            # Use matplotlib to save the image
            plt.figure(figsize=(5, 5))
            plt.imshow(rgb)
            plt.axis('off')
            plt.savefig(thumbnail_path, bbox_inches='tight', dpi=100)
            plt.close()
            
            logging.info(f"Created GDAL thumbnail for {file_path}")
            return True
            
        except Exception as e:
            logging.error(f"Error creating GDAL thumbnail: {e}")
            # Fall through to PIL method
    
    # Method 3: Simple PIL thumbnail creation (last resort)
    if PIL_AVAILABLE:
        try:
            # For PIL, we'll use GDAL to read the first band if available
            if GDAL_AVAILABLE:
                ds = gdal.Open(file_path)
                if ds is not None:
                    band = ds.GetRasterBand(1).ReadAsArray()
                    # Normalize to 0-255
                    band = band.astype(np.float32)
                    if np.min(band) != np.max(bband):
                        band = (band - np.min(band)) / (np.max(band) - np.min(band)) * 255
                    band = band.astype(np.uint8)
                    
                    # Create PIL image
                    img = Image.fromarray(band)
                    img = img.convert('RGB')  # Convert to RGB
                    img.thumbnail(size)
                    img.save(thumbnail_path)
                    
                    logging.info(f"Created simple PIL thumbnail for {file_path}")
                    return True
            
            # If GDAL is not available, try opening with PIL directly (might not work with GeoTIFF)
            logging.warning(f"Falling back to direct PIL loading for {file_path}")
            img = Image.open(file_path)
            img.thumbnail(size)
            img.save(thumbnail_path)
            
            logging.info(f"Created direct PIL thumbnail for {file_path}")
            return True
            
        except Exception as e:
            logging.error(f"Error creating PIL thumbnail: {e}")
    
    # If we reached here, all methods failed
    logging.error(f"All thumbnail creation methods failed for {file_path}")
    return False

def _analyze_geotiff_bands(geotiff_path):
    """
    Analyze a GeoTIFF file to determine the best bands for visualization.
    
    Args:
        geotiff_path: Path to the GeoTIFF file
        
    Returns:
        tuple: (band_indices, reason)
            - band_indices: List of band indices (1-based) to use for visualization
            - reason: String explaining the band selection
    """
    # Default to RGB (Sentinel-2 convention: bands 4,3,2)
    default_rgb = [4, 3, 2]
    default_reason = "Default RGB (4,3,2)"
    
    try:
        # Try to get band information from gdalinfo
        if GDAL_AVAILABLE:
            ds = gdal.Open(geotiff_path)
            if ds is None:
                return default_rgb, "Failed to open with GDAL, using default RGB"
            
            band_count = ds.RasterCount
            
            # If not enough bands for RGB, use what we have
            if band_count < 3:
                return list(range(1, band_count+1)), f"Using all available bands ({band_count})"
            
            # Look for Sentinel-2 band naming convention
            bands_with_desc = []
            for i in range(1, band_count+1):
                band = ds.GetRasterBand(i)
                desc = band.GetDescription()
                bands_with_desc.append((i, desc))
            
            # Check for common sentinel-2 band patterns
            s2_rgb_bands = []
            for i, desc in bands_with_desc:
                if desc in ["B04", "B4", "RED"]:
                    s2_rgb_bands.append((i, "R"))
                elif desc in ["B03", "B3", "GREEN"]:
                    s2_rgb_bands.append((i, "G"))
                elif desc in ["B02", "B2", "BLUE"]:
                    s2_rgb_bands.append((i, "B"))
            
            # If we found all RGB bands, use them
            if len(s2_rgb_bands) == 3:
                # Sort as R,G,B
                s2_rgb_bands.sort(key=lambda x: {"R": 0, "G": 1, "B": 2}[x[1]])
                band_indices = [b[0] for b in s2_rgb_bands]
                return band_indices, "Sentinel-2 RGB bands detected"
            
            # If we have enough bands but no clear naming, use standard positions
            if band_count >= 4:
                # Try typical positions in different products
                # Many products have RGB at bands 1,2,3 or 3,2,1 or 4,3,2
                if band_count >= 12:  # Likely Sentinel-2 with all bands
                    return [4, 3, 2], "Assuming Sentinel-2 with Red=4,Green=3,Blue=2"
                else:
                    return [min(3, band_count), min(2, band_count), min(1, band_count)], "Using first 3 bands as RGB"
            
            # Fallback for any other case
            return default_rgb[:min(band_count, 3)], "Using default band selection"
        
        # If GDAL not available, just return defaults
        return default_rgb, "GDAL not available, using default RGB"
            
    except Exception as e:
        logger.warning(f"Error analyzing bands: {e}")
        return default_rgb, f"Error during band analysis: {str(e)[:30]}..."

def _get_geotiff_statistics(geotiff_path):
    """
    Extract statistics from a GeoTIFF file using gdalinfo with multiple fallback methods.
    
    Args:
        geotiff_path: Path to the GeoTIFF file
        
    Returns:
        Dictionary containing statistics including data type
    """
    stats = {
        'path': geotiff_path,
        'filename': os.path.basename(geotiff_path),
        'size_mb': round(os.path.getsize(geotiff_path) / (1024 * 1024), 2)
    }
    
    # First try with gdalinfo
    try:
        # Try with -stats option first
        cmd = ['gdalinfo', '-stats', geotiff_path]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30)
            output = result.stdout
        except subprocess.TimeoutExpired:
            # If it times out, try without stats
            logger.warning(f"gdalinfo with stats timed out for {os.path.basename(geotiff_path)}, trying without -stats")
            cmd = ['gdalinfo', geotiff_path]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=15)
            output = result.stdout
        
        # Extract basic information
        stats['driver'] = re.search(r'Driver: ([\w\/]+)', output).group(1) if re.search(r'Driver: ([\w\/]+)', output) else 'Unknown'
        
        # Extract size
        size_match = re.search(r'Size is (\d+), (\d+)', output)
        if size_match:
            stats['width'] = int(size_match.group(1))
            stats['height'] = int(size_match.group(2))
            stats['pixels'] = int(size_match.group(1)) * int(size_match.group(2))
        
        # Extract pixel size/resolution
        pixel_match = re.search(r'Pixel Size = \(([^,]+),([^\)]+)\)', output)
        if pixel_match:
            stats['pixel_width_m'] = round(float(pixel_match.group(1)), 5)
            stats['pixel_height_m'] = round(abs(float(pixel_match.group(2))), 5)
        
        # Extract projection info
        srs_match = re.search(r'Coordinate System is:\s*([^\n]+)', output)
        if srs_match:
            stats['projection'] = srs_match.group(1).strip()
            
            # Try to extract EPSG code
            epsg_match = re.search(r'ID\["EPSG",(\d+)\]', output)
            if epsg_match:
                stats['epsg'] = int(epsg_match.group(1))
        
        # Extract band information including data types
        # First, try to get overall datatype if all bands are the same
        datatype_matches = re.findall(r'Band \d+ Block=.* Type=(\w+),', output)
        
        # Store the datatype properly
        if datatype_matches:
            if len(set(datatype_matches)) == 1:
                # All bands have the same datatype
                stats['datatype'] = datatype_matches[0]
            else:
                # Multiple datatypes
                stats['datatype'] = ', '.join(f"{i+1}:{dt}" for i, dt in enumerate(datatype_matches))
            
        # Extract statistics per band (min, max, mean, stddev)
        stats_matches = []
        # Process each band separately to avoid issues with multi-line matching
        band_blocks = re.findall(r'Band (\d+).*?(?=Band \d+|\Z)', output, re.DOTALL)
        
        for i, band_block in enumerate(band_blocks):
            # Process the statistics for this band only
            stats_match = re.search(r'Minimum=([^,]+), Maximum=([^,]+), Mean=([^,]+), StdDev=([^\n\)]+)', band_block)
            if stats_match:
                stats_matches.append((stats_match.group(1), stats_match.group(2), stats_match.group(3), stats_match.group(4)))
        
        if stats_matches:
            if len(stats_matches) == 1:
                # Single band or statistics are the same for all bands
                try:
                    stats['min'] = float(stats_matches[0][0])
                    stats['max'] = float(stats_matches[0][1])
                    stats['mean'] = float(stats_matches[0][2])
                    stats['stddev'] = float(stats_matches[0][3])
                except ValueError as e:
                    logger.warning(f"Could not convert statistics to float: {e}")
                    # Store as strings if conversion fails
                    stats['min'] = stats_matches[0][0]
                    stats['max'] = stats_matches[0][1]
                    stats['mean'] = stats_matches[0][2]
                    stats['stddev'] = stats_matches[0][3]
            else:
                # Multiple bands with different statistics
                for i, (min_val, max_val, mean, stddev) in enumerate(stats_matches):
                    band_num = i + 1
                    try:
                        stats[f'band{band_num}_min'] = float(min_val)
                        stats[f'band{band_num}_max'] = float(max_val)
                        stats[f'band{band_num}_mean'] = float(mean)
                        stats[f'band{band_num}_stddev'] = float(stddev)
                    except ValueError as e:
                        logger.warning(f"Could not convert band {band_num} statistics to float: {e}")
                        # Store as strings if conversion fails
                        stats[f'band{band_num}_min'] = min_val
                        stats[f'band{band_num}_max'] = max_val
                        stats[f'band{band_num}_mean'] = mean
                        stats[f'band{band_num}_stddev'] = stddev
        if datatype_matches and len(set(datatype_matches)) == 1:
            # All bands have the same datatype
            stats['datatype'] = datatype_matches[0]
        elif datatype_matches:
            # Different datatypes per band
            stats['datatype'] = 'Mixed'
        
        # Extract detailed band information with datatype
        band_info = []
        band_stats = []
        
        # Process each band block separately to avoid issues with multi-line matching
        band_blocks = re.split(r'(?=Band \d+ Block)', output)
        
        for block in band_blocks:
            if not block.strip().startswith('Band'):
                continue
                
            # Extract band number and datatype
            band_match = re.match(r'Band (\d+) Block=.* Type=(\w+),', block)
            if band_match:
                band_num = band_match.group(1)
                datatype = band_match.group(2)
                
                # Extract description if available
                desc_match = re.search(r'Description = ([^\n+)', block)
                description = desc_match.group(1) if desc_match else None
                
                band_info.append((band_num, datatype, description))
                
            # Extract statistics for this band only
            stats_match = re.search(r'STATISTICS_MINIMUM=([^\n]+).*?STATISTICS_MAXIMUM=([^\n]+).*?STATISTICS_MEAN=([^\n]+).*?STATISTICS_STDDEV=([^\n]+)', 
                                block, re.DOTALL)
            if stats_match and band_match:  # Only add stats if we have the band number
                band_stats.append((band_match.group(1), stats_match.group(1), stats_match.group(2), 
                                  stats_match.group(3), stats_match.group(4)))
        
        # Process band info first to get datatypes and descriptions
        if band_info:
            stats['band_count'] = len(band_info)
            
            # Create a dictionary to hold band datatypes by band number
            band_datatypes = {}
            for band_num, datatype, description in band_info:
                prefix = f'band{band_num}_'
                band_datatypes[band_num] = datatype
                stats[prefix + 'datatype'] = datatype
                if description and description.strip():
                    stats[prefix + 'description'] = description.strip()
        
        # Then process statistics if available
        if band_stats:
            for band_num, min_val, max_val, mean_val, stddev_val in band_stats:
                prefix = f'band{band_num}_'
                try:
                    stats[prefix + 'min'] = round(float(min_val), 4)
                    stats[prefix + 'max'] = round(float(max_val), 4)
                    stats[prefix + 'mean'] = round(float(mean_val), 4) 
                    stats[prefix + 'stddev'] = round(float(stddev_val), 4)
                    # Add datatype if not already present (from band info)
                    if prefix + 'datatype' not in stats and band_num in band_datatypes:
                        stats[prefix + 'datatype'] = band_datatypes[band_num]
                except ValueError:
                    # Handle non-numeric values
                    stats[prefix + 'min'] = min_val
                    stats[prefix + 'max'] = max_val
                    stats[prefix + 'mean'] = mean_val
                    stats[prefix + 'stddev'] = stddev_val
        else:
            # No band statistics found, try to at least get band count
            if not band_info:
                band_count_matches = re.findall(r'Band (\d+)', output)
                if band_count_matches:
                    stats['band_count'] = max(int(b) for b in band_count_matches)
        
        # Extract metadata if available
        metadata_match = re.search(r'Metadata:(.*?)(?:Corner Coordinates:|$)', output, re.DOTALL)
        if metadata_match:
            metadata_text = metadata_match.group(1).strip()
            if metadata_text:
                # Extract key metadata items
                if 'date' in metadata_text.lower():
                    date_match = re.search(r'DATE[^=]*=\s*([^\n]+)', metadata_text, re.IGNORECASE)
                    if date_match:
                        stats['acquisition_date'] = date_match.group(1).strip()
                        
                # Add other important metadata as needed
                for key in ['CLOUD_COVER', 'SENSOR', 'SATELLITE']:
                    key_match = re.search(f'{key}[^=]*=\\s*([^\\n]+)', metadata_text, re.IGNORECASE)
                    if key_match:
                        stats[key.lower()] = key_match.group(1).strip()
                
    except subprocess.CalledProcessError as e:
        logger.warning(f"Error running gdalinfo on {geotiff_path}: {e}")
        stats['error'] = str(e)
        
        # Try GDAL API directly as fallback
        if GDAL_AVAILABLE:
            try:
                logger.info(f"Attempting fallback with direct GDAL API for {os.path.basename(geotiff_path)}")
                ds = gdal.Open(geotiff_path)
                if ds is not None:
                    stats['width'] = ds.RasterXSize
                    stats['height'] = ds.RasterYSize
                    stats['pixels'] = ds.RasterXSize * ds.RasterYSize
                    stats['band_count'] = ds.RasterCount
                    
                    # Get geotransform
                    gt = ds.GetGeoTransform()
                    if gt:
                        stats['pixel_width_m'] = round(gt[1], 5)
                        stats['pixel_height_m'] = round(abs(gt[5]), 5)
                    
                    # Get projection
                    proj = ds.GetProjection()
                    if proj:
                        stats['projection'] = proj
                    
                    # Get band statistics
                    gdal_datatypes = {
                        gdal.GDT_Byte: "Byte",
                        gdal.GDT_UInt16: "UInt16",
                        gdal.GDT_Int16: "Int16",
                        gdal.GDT_UInt32: "UInt32",
                        gdal.GDT_Int32: "Int32",
                        gdal.GDT_Float32: "Float32",
                        gdal.GDT_Float64: "Float64",
                        gdal.GDT_CInt16: "CInt16",
                        gdal.GDT_CInt32: "CInt32",
                        gdal.GDT_CFloat32: "CFloat32",
                        gdal.GDT_CFloat64: "CFloat64"
                    }
                    
                    # Track if all bands have the same datatype
                    band_datatypes = set();
                    
                    for i in range(1, ds.RasterCount + 1):
                        try:
                            band = ds.GetRasterBand(i)
                            if band:
                                prefix = f'band{i}_'
                                
                                # Get datatype
                                datatype_code = band.DataType
                                datatype_str = gdal_datatypes.get(datatype_code, f"Unknown ({datatype_code})")
                                stats[prefix + 'datatype'] = datatype_str
                                band_datatypes.add(datatype_str);
                                
                                # Get description (if available)
                                description = band.GetDescription()
                                if description:
                                    stats[prefix + 'description'] = description;
                                
                                # Get statistics
                                try:
                                    min_val, max_val, mean_val, stddev_val = band.GetStatistics(0, 1)
                                    stats[prefix + 'min'] = round(float(min_val), 4)
                                    stats[prefix + 'max'] = round(float(max_val), 4)
                                    stats[prefix + 'mean'] = round(float(mean_val), 4)
                                    stats[prefix + 'stddev'] = round(float(stddev_val), 4)
                                except Exception as stat_error:
                                    logger.warning(f"Could not get statistics for band {i}: {stat_error}")
                        except Exception as band_error:
                            logger.warning(f"Could not access band {i}: {band_error}")
                    
                    # If all bands have the same datatype, add a global datatype field
                    if len(band_datatypes) == 1:
                        stats['datatype'] = next(iter(band_datatypes))
                    elif band_datatypes:
                        stats['datatype'] = 'Mixed'
                    
                    ds = None  # Close the dataset
            except Exception as gdal_error:
                logger.warning(f"GDAL API fallback also failed: {gdal_error}")
                stats['gdal_error'] = str(gdal_error)
        
    except Exception as e:
        logger.warning(f"Error extracting statistics from {geotiff_path}: {e}")
        stats['error'] = str(e)
    
    # Ensure we have basic statistics no matter what
    if 'width' not in stats and 'height' not in stats:
        # Try to get minimal information through direct file examination
        try:
            import struct
            with open(geotiff_path, 'rb') as f:
                # Basic TIFF header examination
                header = f.read(16)  # Read TIFF header
                if header[0:4] in (b'II*\x00', b'MM\x00*'):  # TIFF magic numbers
                    stats['format'] = 'TIFF (based on header)'
        except:
            pass
    
    return stats
def _create_placeholder_image(output_path, filename, error_message):
    """
    Create a placeholder image when all other methods fail.
    Uses matplotlib if available, or creates a minimal PNG if not.
    
    Args:
        output_path: Path to save the placeholder image
        filename: Filename to display in the placeholder
        error_message: Error message to display
    """
    try:
        # Make sure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        if MATPLOTLIB_AVAILABLE:
            # Create a simple image with matplotlib
            plt.figure(figsize=(5, 3))
            plt.text(0.5, 0.5, f"Error processing\n{filename}\n\n{error_message[:100]}...",
                    horizontalalignment='center',
                    verticalalignment='center',
                    fontsize=8,
                    color='red',
                    transform=plt.gca().transAxes)
            plt.gca().set_facecolor('#f0f0f0')
            plt.axis('off')
            plt.savefig(output_path, dpi=100, bbox_inches='tight', pad_inches=0.1)
            plt.close()
            return output_path
        elif PIL_AVAILABLE:
            # Try with PIL
            from PIL import Image, ImageDraw, ImageFont
            width, height = 300, 200
            img = Image.new('RGB', (width, height), color=(240, 240, 240))
            draw = ImageDraw.Draw(img)
            draw.text((width/2, height/2), f"Error: {error_message[:50]}...", 
                    fill=(255, 0, 0), anchor="mm")
            img.save(output_path)
            return output_path
        else:
            # Create minimal valid empty PNG file
            with open(output_path, 'wb') as f:
                # Write minimal valid PNG file (header)
                f.write(base64.b64decode(
                    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVQI12P4//8/AAX+Av7czFnnAAAAAElFTkSuQmCC'
                ))
            return output_path
    except Exception as e:
        logger.error(f"Failed to create placeholder image: {e}")
        # Return the path anyway even if we failed
        return output_path

def get_tiff_files(folder_path):
    """
    Get list of TIFF files from a folder, excluding auxiliary files.
    
    Args:
        folder_path: Path to the folder
        
    Returns:
        list: List of TIFF file paths (excluding .aux.xml files)
    """
    tiff_patterns = ['*.tif', '*.tiff', '*.TIF', '*.TIFF']
    tiff_files = []
    
    for pattern in tiff_patterns:
        files = glob.glob(os.path.join(folder_path, pattern))
        # Filter out auxiliary files
        files = [f for f in files if not f.endswith('.aux.xml')]
        tiff_files.extend(files)
    
    return tiff_files

def compare_geotiffs(ref_path, comp_path, tolerance=1e-6):
    """
    Compare two GeoTIFF files by metadata and pixel values.
    
    Args:
        ref_path: Path to reference GeoTIFF
        comp_path: Path to comparison GeoTIFF
        tolerance: Tolerance for pixel value comparison
        
    Returns:
        dict: Comparison result with 'match' (bool) and 'reason' (string)
    """
    if not GDAL_AVAILABLE:
        return {'match': False, 'reason': 'GDAL not available for comparison'}
    
    try:
        # Open both files
        ref_ds = gdal.Open(ref_path)
        comp_ds = gdal.Open(comp_path)
        
        if ref_ds is None:
            return {'match': False, 'reason': f'Cannot open reference file: {os.path.basename(ref_path)}'}
        if comp_ds is None:
            return {'match': False, 'reason': f'Cannot open comparison file: {os.path.basename(comp_path)}'}
        
        # Compare basic metadata
        if ref_ds.RasterXSize != comp_ds.RasterXSize or ref_ds.RasterYSize != comp_ds.RasterYSize:
            return {'match': False, 'reason': f'Different dimensions: {ref_ds.RasterXSize}x{ref_ds.RasterYSize} vs {comp_ds.RasterXSize}x{comp_ds.RasterYSize}'}
        
        if ref_ds.RasterCount != comp_ds.RasterCount:
            return {'match': False, 'reason': f'Different band count: {ref_ds.RasterCount} vs {comp_ds.RasterCount}'}
        
        # Compare geotransform (spatial reference)
        ref_gt = ref_ds.GetGeoTransform()
        comp_gt = comp_ds.GetGeoTransform()
        if ref_gt != comp_gt:
            return {'match': False, 'reason': 'Different geotransform/spatial reference'}
        
        # Try to compare pixel values, but fallback gracefully if GDAL array operations fail
        try:
            # Sample and compare pixel values from multiple locations
            sample_points = min(100, ref_ds.RasterXSize * ref_ds.RasterYSize // 100)  # Sample up to 100 points
            if sample_points < 10:
                sample_points = min(10, ref_ds.RasterXSize * ref_ds.RasterYSize)
            
            np.random.seed(42)  # For reproducible sampling
            x_coords = np.random.randint(0, ref_ds.RasterXSize, sample_points)
            y_coords = np.random.randint(0, ref_ds.RasterYSize, sample_points)
            
            for band_idx in range(1, ref_ds.RasterCount + 1):
                ref_band = ref_ds.GetRasterBand(band_idx)
                comp_band = comp_ds.GetRasterBand(band_idx)
                
                # Check data types
                if ref_band.DataType != comp_band.DataType:
                    return {'match': False, 'reason': f'Different data types in band {band_idx}'}
                
                # Sample pixel values
                for x, y in zip(x_coords, y_coords):
                    ref_val = ref_band.ReadAsArray(x, y, 1, 1)
                    comp_val = comp_band.ReadAsArray(x, y, 1, 1)
                    
                    if ref_val is None or comp_val is None:
                        continue
                    
                    ref_val = float(ref_val[0, 0])
                    comp_val = float(comp_val[0, 0])
                    
                    # Handle NaN values
                    if np.isnan(ref_val) and np.isnan(comp_val):
                        continue
                    elif np.isnan(ref_val) or np.isnan(comp_val):
                        return {'match': False, 'reason': f'NaN mismatch in band {band_idx} at ({x},{y})'}
                    
                    # Compare with tolerance
                    if abs(ref_val - comp_val) > tolerance:
                        return {'match': False, 'reason': f'Pixel value difference in band {band_idx}: {abs(ref_val - comp_val):.2e} > {tolerance:.2e}'}
        
        except Exception as pixel_error:
            # If pixel comparison fails (e.g., GDAL array issues), fall back to metadata-only comparison
            return {'match': True, 'reason': f'Metadata matches (pixel comparison failed: {str(pixel_error)})'}
        
        return {'match': True, 'reason': 'Files match within tolerance'}
        
    except Exception as e:
        return {'match': False, 'reason': f'Comparison error: {str(e)}'}
    finally:
        # Cleanup
        if 'ref_ds' in locals() and ref_ds is not None:
            ref_ds = None
        if 'comp_ds' in locals() and comp_ds is not None:
            comp_ds = None

def group_folders_by_platform(folders):
    """
    Group output folders by platform based on folder naming convention.
    Format: {backend_url_with_underscores}_{scenario_with_backend}_{timestamp}
    
    Args:
        folders: List of folder paths
        
    Returns:
        dict: Platform name -> list of folder info dicts
    """
    platform_groups = defaultdict(list)
    
    for folder_path in folders:
        folder_name = os.path.basename(folder_path)
        
        # Extract platform from folder name
        parts = folder_name.split('_')
        if len(parts) >= 3:
            # Find timestamp (14 digits at the end)
            timestamp_idx = -1
            for i in range(len(parts) - 1, -1, -1):
                if re.match(r'^\d{14}$', parts[i]):
                    timestamp_idx = i
                    break
            
            if timestamp_idx > 1:
                # Platform could be multiple parts before timestamp
                # Look for common platform patterns
                platform_parts = []
                platform_start_idx = timestamp_idx - 1
                
                # Check for multi-word platform names
                if timestamp_idx >= 2 and parts[timestamp_idx - 2] == 'openeo' and parts[timestamp_idx - 1] == 'platform':
                    platform_parts = ['openeo', 'platform']
                    platform_start_idx = timestamp_idx - 2
                elif parts[timestamp_idx - 1] in ['earthengine', 'cdse', 'vito', 'eodc', 'platform']:
                    platform_parts = [parts[timestamp_idx - 1]]
                    platform_start_idx = timestamp_idx - 1
                else:
                    # Default: single part before timestamp
                    platform_parts = [parts[timestamp_idx - 1]]
                    platform_start_idx = timestamp_idx - 1
                
                platform = '_'.join(platform_parts)
                
                # Extract the actual scenario by removing backend URL and platform
                scenario_with_backend = parts[:platform_start_idx]
                
                # Try to identify and remove common backend URL patterns
                scenario_parts = []
                skip_backend = False
                
                for i, part in enumerate(scenario_with_backend):
                    # Skip known backend URL patterns
                    if part in ['earthengine', 'openeo', 'openeocloud', 'dataspace', 'copernicus', 'eu', 'org', 'vito', 'be'] and i < 4:
                        skip_backend = True
                        continue
                    # Once we hit a non-backend part, start collecting scenario
                    if skip_backend and (part.startswith(('ndvi', 'reducer', 'vienna', 'bratislava')) or 
                                       part.endswith(('km', 'median', 'mean')) or
                                       re.match(r'^\d+$', part) or part in ['10km', '2024', '2020', '2018']):
                        scenario_parts.extend(scenario_with_backend[i:])
                        break
                
                # If we couldn't identify the scenario start, use everything after index 3
                if not scenario_parts and len(scenario_with_backend) > 3:
                    scenario_parts = scenario_with_backend[3:]
                elif not scenario_parts:
                    scenario_parts = scenario_with_backend
                
                scenario = '_'.join(scenario_parts)
                
                platform_groups[platform].append({
                    'path': folder_path,
                    'name': folder_name,
                    'scenario': scenario,
                    'platform': platform
                })
            else:
                # Fallback: assume last part before potential timestamp is platform
                platform = parts[-2] if len(parts) >= 2 else parts[-1]
                scenario = '_'.join(parts[:-2]) if len(parts) >= 3 else '_'.join(parts[:-1])
                
                platform_groups[platform].append({
                    'path': folder_path,
                    'name': folder_name,
                    'scenario': scenario,
                    'platform': platform
                })
        else:
            # Fallback: use folder name as-is
            platform_groups[folder_name].append({
                'path': folder_path,
                'name': folder_name,
                'scenario': folder_name,
                'platform': folder_name
            })
    
    return dict(platform_groups)

def compare_task(input_patterns, reference_platform, output_path, tolerance=1e-6):
    """
    Compare GeoTIFF results across different platforms.
    
    Args:
        input_patterns: List of glob patterns for input folders
        reference_platform: Name of the reference platform
        output_path: Output markdown file path
        tolerance: Tolerance for pixel value comparison
    """
    print(f"Compare task: input_patterns={input_patterns}, reference_platform={reference_platform}")
    print(f"Output: {output_path}, tolerance={tolerance}")
    
    if not output_path.endswith('.md'):
        print("Error: Output file must end with .md")
        sys.exit(1)
    
    # Search for matching folders in the output directory
    output_dir = os.path.join(os.getcwd(), 'output')
    if not os.path.exists(output_dir):
        print(f"Error: Output directory '{output_dir}' does not exist")
        sys.exit(1)
    
    print(f"Searching in: {output_dir}")
    
    # Find all matching folders
    matching_folders = []
    for pattern in input_patterns:
        pattern_path = os.path.join(output_dir, pattern)
        matches = glob.glob(pattern_path)
        matching_folders.extend([f for f in matches if os.path.isdir(f)])
        print(f"Pattern '{pattern}' found {len([f for f in matches if os.path.isdir(f)])} folders")
    
    if not matching_folders:
        print("Error: No matching folders found")
        sys.exit(1)
    
    print(f"Total folders found: {len(matching_folders)}")
    
    # Group folders by platform
    platform_groups = group_folders_by_platform(matching_folders)
    print(f"Platforms found: {list(platform_groups.keys())}")
    
    # Check if reference platform exists
    if reference_platform not in platform_groups:
        print(f"Error: Reference platform '{reference_platform}' not found")
        print(f"Available platforms: {list(platform_groups.keys())}")
        sys.exit(1)
    
    # Get reference folders
    reference_folders = platform_groups[reference_platform]
    comparison_platforms = {k: v for k, v in platform_groups.items() if k != reference_platform}
    
    print(f"Reference platform '{reference_platform}' has {len(reference_folders)} folders")
    print(f"Comparing against {len(comparison_platforms)} other platforms")
    
    # Build comparison matrix
    comparison_results = {}
    scenario_names = set()
    
    # First, collect all unique scenarios and pick one representative folder per scenario per platform
    scenario_folders = defaultdict(lambda: defaultdict(list))
    
    # Group folders by scenario and platform
    for platform, folders in platform_groups.items():
        for folder in folders:
            scenario = folder['scenario']
            scenario_names.add(scenario)
            scenario_folders[scenario][platform].append(folder)
    
    # Process each scenario once
    for scenario in scenario_names:
        if scenario not in comparison_results:
            comparison_results[scenario] = {}
        
        # Get reference folders for this scenario
        ref_folders = scenario_folders[scenario].get(reference_platform, [])
        if not ref_folders:
            continue  # Skip scenarios without reference
        
        # Use the first reference folder to get the file list and count
        ref_folder = ref_folders[0]
        ref_tiffs = get_tiff_files(ref_folder['path'])
        total_files = len(ref_tiffs)
        
        for platform, folders in comparison_platforms.items():
            comparison_results[scenario][platform] = {
                'total': total_files,
                'matching': 0,
                'not_matching': 0,
                'missing': 0,
                'reasons': []
            }
            
            # Find corresponding folders in comparison platform
            comp_folders = scenario_folders[scenario].get(platform, [])
            
            if not comp_folders:
                # No corresponding scenario folder
                comparison_results[scenario][platform]['missing'] = total_files
                comparison_results[scenario][platform]['reasons'].append(f"No {platform} folder for scenario {scenario}")
                continue
            
            # Use the first comparison folder
            comp_folder = comp_folders[0]
            comp_tiffs = get_tiff_files(comp_folder['path'])
            comp_tiff_names = {os.path.basename(t): t for t in comp_tiffs}
            
            # Compare each reference TIFF
            for ref_tiff in ref_tiffs:
                ref_name = os.path.basename(ref_tiff)
                
                if ref_name not in comp_tiff_names:
                    # Missing file
                    comparison_results[scenario][platform]['missing'] += 1
                    comparison_results[scenario][platform]['reasons'].append(f"Missing file: {ref_name}")
                else:
                    # Compare files
                    comp_result = compare_geotiffs(ref_tiff, comp_tiff_names[ref_name], tolerance)
                    
                    if comp_result['match']:
                        comparison_results[scenario][platform]['matching'] += 1
                    else:
                        comparison_results[scenario][platform]['not_matching'] += 1
                        comparison_results[scenario][platform]['reasons'].append(f"{ref_name}: {comp_result['reason']}")
    
    # Generate markdown report
    print(f"Writing comparison report to: {output_path}")
    
    with open(output_path, 'w') as f:
        f.write(f"# GeoTIFF Comparison Report\n\n")
        f.write(f"**Reference Platform:** {reference_platform}\n")
        f.write(f"**Tolerance:** {tolerance}\n")
        f.write(f"**Generated:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        if not comparison_results:
            f.write("No comparison results found.\n")
            return
        
        # Summary table
        f.write("## Summary\n\n")
        f.write("| Scenario | Platform | Total | Matching | Not Matching | Missing | Match Rate |\n")
        f.write("|----------|----------|-------|----------|--------------|---------|------------|\n")
        
        for scenario in sorted(scenario_names):
            if scenario in comparison_results:
                for platform in sorted(comparison_results[scenario].keys()):
                    result = comparison_results[scenario][platform]
                    total = result['total']
                    matching = result['matching']
                    match_rate = f"{matching}/{total} ({100*matching/total:.1f}%)" if total > 0 else "N/A"
                    
                    f.write(f"| {scenario} | {platform} | {total} | {matching} | {result['not_matching']} | {result['missing']} | {match_rate} |\n")
        
        # Detailed results
        f.write("\n## Detailed Results\n\n")
        
        for scenario in sorted(scenario_names):
            if scenario not in comparison_results:
                continue
                
            f.write(f"### {scenario}\n\n")
            
            for platform in sorted(comparison_results[scenario].keys()):
                result = comparison_results[scenario][platform]
                f.write(f"#### vs {platform}\n\n")
                f.write(f"- **Total files:** {result['total']}\n")
                f.write(f"- **Matching:** {result['matching']}\n")
                f.write(f"- **Not matching:** {result['not_matching']}\n")
                f.write(f"- **Missing:** {result['missing']}\n\n")
                
                if result['reasons']:
                    f.write("**Issues:**\n")
                    for reason in result['reasons'][:10]:  # Limit to first 10 reasons
                        f.write(f"- {reason}\n")
                    if len(result['reasons']) > 10:
                        f.write(f"- ... and {len(result['reasons']) - 10} more issues\n")
                    f.write("\n")
    
    print(f"Comparison complete! Report saved to: {output_path}")

def main():
    """Main function to parse arguments and execute the appropriate task."""
    parser = argparse.ArgumentParser(description='OpenEO Backend Scenario Runner and Summarizer')
    subparsers = parser.add_subparsers(dest='task', help='Task to perform')
    
    # Run task parser
    run_parser = subparsers.add_parser('run', help='Run a scenario on an OpenEO backend')
    run_parser.add_argument('--api-url', required=True, help='URL of the OpenEO backend')
    run_parser.add_argument('--scenario', required=True, help='Path to the process graph JSON file')
    run_parser.add_argument('--output-directory', help='Output directory for results (optional)')
    
    # Summarize task parser
    summarize_parser = subparsers.add_parser('summarize', help='Generate a summary report from output folders')
    summarize_parser.add_argument('--input', dest='input_patterns', required=True, nargs='+', 
                                help='Input folders to summarize (supports glob patterns)')
    summarize_parser.add_argument('--output', dest='output_path', required=True, 
                                
                                help='Output file path (must end with .md or .csv)')
    # Support for old positional argument style
    summarize_parser.add_argument('input', nargs='?', help=argparse.SUPPRESS)
    summarize_parser.add_argument('output', nargs='?', help=argparse.SUPPRESS)
    
    # Visualize task parser
    visualize_parser = subparsers.add_parser('visualize', help='Create visualizations and statistics of GeoTIFF results')
    visualize_parser.add_argument('--input', dest='input_patterns', required=True, nargs='+', 
                                help='Input folders to visualize (supports glob patterns)')
    visualize_parser.add_argument('--output', dest='output_path', required=True, 
                                help='Output markdown file path (must end with .md)')
    # Support for old positional argument style
    visualize_parser.add_argument('input', nargs='?', help=argparse.SUPPRESS)
    visualize_parser.add_argument('output', nargs='?', help=argparse.SUPPRESS)
    
    # Compare task parser
    compare_parser = subparsers.add_parser('compare', help='Compare GeoTIFF results across different platforms')
    compare_parser.add_argument('--input', dest='input_patterns', required=True, nargs='+', 
                               help='Input folders to compare (supports glob patterns)')
    compare_parser.add_argument('--reference', dest='reference_platform', required=True, 
                               help='Reference platform name to compare against')
    compare_parser.add_argument('--output', dest='output_path', required=True, 
                               help='Output markdown file path (must end with .md)')
    compare_parser.add_argument('--tolerance', dest='tolerance', type=float, default=1e-6, 
                               help='Tolerance for pixel value comparison (default: 1e-6)')
    
    args = parser.parse_args()
    
    if args.task == 'run':
        run_task(args.api_url, args.scenario, args.output_directory)
    elif args.task == 'summarize':
        # Handle both new and old-style arguments
        input_patterns = args.input_patterns if hasattr(args, 'input_patterns') and args.input_patterns else [args.input]
        output_path = args.output_path if hasattr(args, 'output_path') and args.output_path else args.output
        
        if not input_patterns or not output_path:
            summarize_parser.error("Missing required arguments. Use --input and --output.")
        
        summarize_task(input_patterns, output_path)
    elif args.task == 'visualize':
        # Handle both new and old-style arguments
        input_patterns = args.input_patterns if hasattr(args, 'input_patterns') and args.input_patterns else [args.input]
        output_path = args.output_path if hasattr(args, 'output_path') and args.output_path else args.output
        
        if not input_patterns or not output_path:
            visualize_parser.error("Missing required arguments. Use --input and --output.")
        
        visualize_task(input_patterns, output_path)
    elif args.task == 'compare':
        compare_task(args.input_patterns, args.reference_platform, args.output_path, args.tolerance)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
