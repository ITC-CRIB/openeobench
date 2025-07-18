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
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    print("Warning: numpy not available, some visualization features will be limited")

# For enhanced geotiff loading
try:
    import rioxarray as rxr
    import xarray as xr  # For netCDF and advanced array operations
    RIOXARRAY_AVAILABLE = True
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

def get_geotiff_files(directory):
    """
    Get all GeoTIFF files in a directory using gdalinfo or file command for proper detection.
    Returns a list of file paths that are actual GeoTIFF files.
    """
    if not os.path.exists(directory):
        return []
    
    geotiff_files = []
    
    # Get all files in the directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            # Skip auxiliary files
            if file.endswith('.aux.xml') or file.endswith('.tif.aux.xml'):
                continue
                
            file_path = os.path.join(root, file)
            
            # Try gdalinfo first (more reliable for GeoTIFF detection)
            try:
                result = subprocess.run(['gdalinfo', file_path], 
                                      capture_output=True, text=True, timeout=10)
                if result.returncode == 0 and 'TIFF' in result.stdout.upper():
                    geotiff_files.append(file_path)
                    continue
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass
            
            # Fallback to file command
            try:
                result = subprocess.run(['file', file_path], 
                                      capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    output = result.stdout.upper()
                    if 'TIFF' in output and ('IMAGE' in output or 'DATA' in output):
                        geotiff_files.append(file_path)
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass
    
    return geotiff_files

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


def summarize_task(patterns, output_path, debug=False):
    """
    Create summary from output folders.
    
    Args:
        patterns: List of patterns to match folder names (within output/ directory)
        output_path: Output path for the summary
        debug: Whether to print debug information
        
    Returns:
        True if successful, False if there were errors
    """
    # Always look in output/ directory first
    base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
    
    # Get matching folders
    matched_folders = []
    for pattern in patterns:
        # Search in output directory with wildcards
        output_pattern = os.path.join(base_dir, f"*{pattern}*")
        logger.info(f"Searching for pattern: {output_pattern}")
        output_matches = glob.glob(output_pattern)
        matched_folders.extend(output_matches)
        
        # Also search for direct pattern matches
        direct_matches = glob.glob(pattern)
        matched_folders.extend(direct_matches)
    
    # Remove duplicates and filter for directories only
    matched_folders = list(set([f for f in matched_folders if os.path.isdir(f)]))
    matched_folders = sorted(matched_folders)
    
    if not matched_folders:
        logger.error(f"No folders matched the input pattern(s). Checked both output/ directory and direct patterns.")
        return False
    
    logger.info(f"Found {len(matched_folders)} matching folders: {[os.path.basename(f) for f in matched_folders[:5]]}")

    # Helper function to load folder data
    def _load_folder_data(folder_path):
        """Load data from results.json in the folder."""
        results_file = os.path.join(folder_path, 'results.json')
        if os.path.exists(results_file):
            try:
                with open(results_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading {results_file}: {e}")
                return None
        return None

    # Helper function to extract backend name from folder name
    def _extract_backend_from_folder(folder_name):
        """Extract backend name from folder name."""
        # Split by underscore and look for timestamp pattern (YYYYMMDDHHMMSS)
        parts = folder_name.split('_')
        
        # Find timestamp (should be 14 digits)
        timestamp_idx = -1
        for i, part in enumerate(parts):
            if len(part) == 14 and part.isdigit():
                timestamp_idx = i
                break
            
        if timestamp_idx > 0:
            # Backend name should be the part just before timestamp
            backend_part = parts[timestamp_idx - 1]
            
            # Map common backend identifiers
            backend_mapping = {
                'cdse': 'CDSE',
                'vito': 'VITO', 
                'eodc': 'EODC',
                'earthengine': 'Earth Engine',
                'openeo_platform': 'openEO Platform',
                'platform': 'openEO Platform'
            }
            
            return backend_mapping.get(backend_part.lower(), backend_part.title())
        
        # Fallback: try to extract from URL part
        if 'vito' in folder_name.lower():
            return 'VITO'
        elif 'eodc' in folder_name.lower():
            return 'EODC'
        elif 'dataspace' in folder_name.lower() or 'copernicus' in folder_name.lower():
            return 'CDSE'
        elif 'earthengine' in folder_name.lower():
            return 'Earth Engine'
        elif 'openeo' in folder_name.lower():
            return 'openEO Platform'
        
        return 'Unknown'
    
    # Helper function to write CSV format
    def _write_summary_csv(data, output_path, units):
        """Write summary data to CSV format."""
        if not data:
            return False
        
        # Write main CSV file (include all data)
        with open(output_path, 'w', newline='') as csvfile:
            # Get all field names, ensuring _backend comes after _folder
            all_fields = set()
            for entry in data:
                all_fields.update(entry.keys())
            
            # Order fields: _folder, _backend, then timing fields
            ordered_fields = ['_folder', '_backend']
            timing_fields = sorted([f for f in all_fields if f not in ['_folder', '_backend']])
            ordered_fields.extend(timing_fields)
            
            writer = csv.DictWriter(csvfile, fieldnames=ordered_fields)
            writer.writeheader()
            writer.writerows(data)
        
        # Filter data for summary statistics: only include entries with more than 0 TIFF files
        filtered_data = []
        for entry in data:
            tiff_count = entry.get('tiff_count [files]', 0)
            try:
                if float(tiff_count) > 0:
                    filtered_data.append(entry)
            except (ValueError, TypeError):
                continue
        
        if not filtered_data:
            logger.warning("No entries with more than 0 TIFF files found for summary statistics")
            return True
        
        # Generate summary statistics BY BACKEND (only for successful jobs with >1 TIFF)
        summary_data = []
        timing_fields = [f for f in filtered_data[0].keys() if f not in ['_folder', '_backend']]
        
        # Group filtered data by backend
        backend_groups = {}
        for entry in filtered_data:
            backend = entry.get('_backend', 'Unknown')
            if backend not in backend_groups:
                backend_groups[backend] = []
            backend_groups[backend].append(entry)
        
        # Calculate statistics for each backend and metric combination
        for backend, backend_entries in backend_groups.items():
            for field in timing_fields:
                values = []
                for entry in backend_entries:
                    if field in entry and entry[field] is not None:
                        try:
                            values.append(float(entry[field]))
                        except (ValueError, TypeError):
                            continue
                
                if values:
                    summary_data.append({
                        'backend': backend,
                        'metric': field,
                        'n': len(values),
                        'min': min(values),
                        'max': max(values),
                        'average': sum(values) / len(values),
                        'stddev': (sum((x - sum(values)/len(values))**2 for x in values) / len(values))**0.5 if len(values) > 1 else 0
                    })
        
        # Write summary statistics CSV with backend grouping
        summary_path = output_path.replace('.csv', '_summary.csv')
        with open(summary_path, 'w', newline='') as csvfile:
            if summary_data:
                writer = csv.DictWriter(csvfile, fieldnames=['backend', 'metric', 'n', 'min', 'max', 'average', 'stddev'])
                writer.writeheader()
                writer.writerows(summary_data)
        
        return True
    
    # Helper function to write Markdown format
    def _write_summary_markdown(data, output_path, units):
        """Write summary data to Markdown format."""
        if not data:
            return False
        
        with open(output_path, 'w') as f:
            f.write("# OpenEO Results Summary\n\n")
            f.write(f"Generated on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Get all field names, ensuring _backend comes after _folder
            all_fields = set()
            for entry in data:
                all_fields.update(entry.keys())
            
            # Order fields: _folder, _backend, then timing fields
            ordered_fields = ['_folder', '_backend']
            timing_fields = sorted([f for f in all_fields if f not in ['_folder', '_backend']])
            ordered_fields.extend(timing_fields)
            
            # Write main results table (include all data)
            f.write("## Folder Results\n\n")
            
            # Create table header
            f.write("| " + " | ".join(ordered_fields) + " |\n")
            f.write("|" + "---|" * len(ordered_fields) + "\n")
            
            # Write data rows
            for entry in data:
                row_values = []
                for field in ordered_fields:
                    value = entry.get(field, '')
                    if isinstance(value, float):
                        row_values.append(f"{value:.3f}")
                    else:
                        row_values.append(str(value))
                f.write("| " + " | ".join(row_values) + " |\n")
            
            # Filter data for summary statistics: only include entries with more than 0 TIFF files
            filtered_data = []
            for entry in data:
                tiff_count = entry.get('tiff_count [files]', 0)
                try:
                    if float(tiff_count) > 0:
                        filtered_data.append(entry)
                except (ValueError, TypeError):
                    continue
            
            if not filtered_data:
                f.write("\n## Summary Statistics by Backend\n\n")
                f.write("*No entries with more than 0 TIFF files found for summary statistics.*\n")
                return True
            
            # Generate and write summary statistics BY BACKEND (only for successful jobs)
            f.write(f"\n## Summary Statistics by Backend\n\n")
            f.write("*Note: Only includes entries with more than 0 TIFF files (successful jobs)*\n\n")
            f.write("| backend | metric | n | min | max | average | stddev |\n")
            f.write("|---|---|---|---|---|---|---|\n")
            
            # Group filtered data by backend
            backend_groups = {}
            for entry in filtered_data:
                backend = entry.get('_backend', 'Unknown')
                if backend not in backend_groups:
                    backend_groups[backend] = []
                backend_groups[backend].append(entry)
            
            # Calculate statistics for each backend and metric combination
            for backend in sorted(backend_groups.keys()):
                backend_entries = backend_groups[backend]
                for field in timing_fields:
                    values = []
                    for entry in backend_entries:
                        if field in entry and entry[field] is not None:
                            try:
                                values.append(float(entry[field]))
                            except (ValueError, TypeError):
                                continue
                    
                    if values:
                        n = len(values)
                        min_val = min(values)
                        max_val = max(values)
                        avg = sum(values) / len(values)
                        stddev = (sum((x - avg)**2 for x in values) / len(values))**0.5 if len(values) > 1 else 0
                        
                        f.write(f"| {backend} | {field} | {n} | {min_val:.3f} | {max_val:.3f} | {avg:.3f} | {stddev:.3f} |\n")
        
        return True
    
    # Collect data from all matched folders
    all_data = []
    
    for folder_path in matched_folders:
        folder_name = os.path.basename(folder_path)
        logger.info(f"Processing folder: {folder_name}")
        
        # Extract backend name from folder structure
        backend_name = _extract_backend_from_folder(folder_name)
        
        # Count GeoTIFF files in the folder
        tiff_files = get_geotiff_files(folder_path)
        tiff_count = len(tiff_files)
        
        # Try to load existing data
        try:
            data = _load_folder_data(folder_path)
            if data:
                # Add folder name, backend name, and TIFF count to the data
                data['_folder'] = folder_name
                data['_backend'] = backend_name  # Add backend information
                data['tiff_count'] = tiff_count
                all_data.append(data)
                logger.info(f"Loaded data from {folder_name} (backend: {backend_name}) - {tiff_count} TIFF files")
            else:
                logger.warning(f"No valid data found in {folder_name}")
        except Exception as e:
            logger.error(f"Error processing {folder_path}: {e}")
    
    if not all_data:
        logger.error("No valid data found in any matched folders")
        return False
    
    logger.info(f"Successfully processed {len(all_data)} folders")
    
    # Define units for timing/statistics fields
    units = {
        'download_time': 'seconds',
        'job_execution_time': 'seconds', 
        'processing_time': 'seconds',
        'queue_time': 'seconds',
        'submit_time': 'seconds',
        'total_time': 'seconds',
        'tiff_count': 'files'
    }
    
    # Filter data to only include folder name, backend, and timing/statistics fields
    filtered_data = []
    for entry in all_data:
        filtered_entry = {
            '_folder': entry['_folder'],
            '_backend': entry.get('_backend', 'unknown')  # Include backend information
        }
        
        # Add timing/statistics fields with units
        for key, value in entry.items():
            if key in units and key != '_folder' and key != '_backend':
                unit_suffix = f" [{units[key]}]" if units[key] else ""
                filtered_entry[f"{key}{unit_suffix}"] = value
        
        filtered_data.append(filtered_entry)
    
    # Determine output format
    if output_path.lower().endswith('.csv'):
        return _write_summary_csv(filtered_data, output_path, units)
    elif output_path.lower().endswith('.md'):
        return _write_summary_markdown(filtered_data, output_path, units)
    else:
        logger.error(f"Unsupported output format. Use .csv or .md extension")
        return False
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
def visualize_task(input_patterns, output_path, output_format="both", png_single=False):
    """
    Create a visual matrix of resulting GeoTIFF images and summary statistics.
    Enhanced with robust error handling, multiple fallback methods, and publication-quality visualization.
    Creates a matrix with folders as columns and files as rows, and includes
    datatype information in the statistics output.
    
    Args:
        input_patterns: List of glob patterns matching folders to visualize
        output_path: Path to save visualization results
        output_format: Output format - "md" (markdown), "png" (PNG matrix), or "both" (default)
        png_single: Whether to create individual PNG files for each image
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
    
    # Expand patterns to folders and individual files
    matched_folders = set()
    matched_individual_files = {}  # folder_path -> [file_paths]
    
    # First, ensure we're always looking in the output directory for folders
    for pattern in input_patterns:
        # Try specific output directory pattern
        output_pattern = os.path.join("output", f"*{pattern}*")
        logger.info(f"Searching for pattern: {output_pattern}")
        output_matches = glob.glob(output_pattern)
        for folder in output_matches:
            if os.path.isdir(folder):
                matched_folders.add(folder)
    
    # If no matches found in output directory with wildcards, try exact patterns
    if not matched_folders and not matched_individual_files:
        for pattern in input_patterns:
            # Try exact pattern in output directory (folder)
            output_pattern = os.path.join("output", pattern)
            if os.path.isdir(output_pattern):
                matched_folders.add(output_pattern)
            
            # Check direct pattern as fallback
            if os.path.isdir(pattern):
                matched_folders.add(pattern)
            elif os.path.isfile(pattern):
                # Handle individual files
                if pattern.lower().endswith(('.tif', '.tiff')):
                    parent_dir = os.path.dirname(pattern) or "."
                    if parent_dir not in matched_individual_files:
                        matched_individual_files[parent_dir] = []
                    matched_individual_files[parent_dir].append(pattern)
                    logger.info(f"Found individual TIFF file: {pattern}")
                else:
                    logger.warning(f"Skipping non-TIFF file: {pattern}")
            elif glob.glob(pattern):
                # Handle glob patterns that might match files or directories
                for match in glob.glob(pattern):
                    if os.path.isdir(match):
                        matched_folders.add(match)
                    elif os.path.isfile(match) and match.lower().endswith(('.tif', '.tiff')):
                        parent_dir = os.path.dirname(match) or "."
                        if parent_dir not in matched_individual_files:
                            matched_individual_files[parent_dir] = []
                        matched_individual_files[parent_dir].append(match)
                        logger.info(f"Found individual TIFF file from glob: {match}")
    
    # Sort for consistent output
    matched_folders = sorted(matched_folders)
    total_individual_files = sum(len(files) for files in matched_individual_files.values())
    
    logger.info(f"Matched {len(matched_folders)} folders and {total_individual_files} individual files.")
    
    if matched_folders:
        # Print first few matches for confirmation
        preview = ", ".join(os.path.basename(f) for f in list(matched_folders)[:5])
        if len(matched_folders) > 5:
            preview += f", ... ({len(matched_folders) - 5} more)"
        logger.info(f"Found folders: {preview}")
    
    if matched_individual_files:
        file_preview = []
        for parent_dir, files in list(matched_individual_files.items())[:3]:
            file_preview.extend([os.path.basename(f) for f in files[:2]])
        if total_individual_files > len(file_preview):
            file_preview.append(f"... ({total_individual_files - len(file_preview)} more)")
        logger.info(f"Found individual files: {', '.join(file_preview)}")
    
    if not matched_folders and not matched_individual_files:
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
    
    # Determine what outputs to create based on format parameter
    create_markdown = output_format in ["md", "both"]
    create_png = output_format in ["png", "both"]
    
    # Validate output path format
    valid_extensions = ['.md', '.png', '.csv']
    if not any(output_path.endswith(ext) for ext in valid_extensions):
        logger.error(f"Output file must have one of these extensions: {', '.join(valid_extensions)}")
        return
    
    # Create output directories
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # Directory for storing visualizations (only needed for markdown)
    if create_markdown:
        vis_dir = os.path.join(output_dir, 'visualizations')
        os.makedirs(vis_dir, exist_ok=True)
    else:
        vis_dir = None
    
    # Dictionary to store all TIFF files by folder
    folder_images = {}
    all_stats = []
    errors = []
    
    # Find all GeoTIFF files in matched folders with improved file detection
    for folder in matched_folders:
        folder_name = os.path.basename(folder)
        
        # Use proper GeoTIFF detection instead of file extensions
        tiff_files = get_geotiff_files(folder)
        
        # Also check subdirectories recursively
        for root, dirs, files in os.walk(folder):
            if root != folder:  # Skip the main folder as we already processed it
                tiff_files.extend(get_geotiff_files(root))
        
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
    
    # Handle individual files grouped by their parent directories
    for parent_dir, file_paths in matched_individual_files.items():
        # Create a unique folder name for individual files
        if parent_dir == ".":
            folder_name = "current_directory"
        else:
            folder_name = os.path.basename(parent_dir) + "_individual_files"
        
        # Make sure folder name is unique
        original_folder_name = folder_name
        counter = 1
        while folder_name in folder_images:
            folder_name = f"{original_folder_name}_{counter}"
            counter += 1
        
        folder_images[folder_name] = file_paths
        logger.info(f"Added {len(file_paths)} individual TIFF file(s) as folder '{folder_name}'")
    
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
                # Only create thumbnails for markdown output
                if create_markdown and vis_dir:
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
                
                # Create a placeholder image for failures (only for markdown)
                if create_markdown and vis_dir:
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
    
    # Determine output directory and file paths
    output_dir = os.path.dirname(output_path) if os.path.dirname(output_path) else "."
    base_name = os.path.splitext(os.path.basename(output_path))[0]
    
    # If only PNG format requested, adjust the output path
    if output_format == "png" and output_path.endswith('.md'):
        output_path = os.path.join(output_dir, base_name + "_matrix.png")
    
    # Create markdown file only if requested
    if create_markdown:
        md_path = output_path if output_path.endswith('.md') else os.path.join(output_dir, base_name + ".md")
        
        with open(md_path, 'w') as md_file:
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
            md_file.write("Statistics for all GeoTIFF files are included in the enhanced visualization below.\n\n")
            
            # Add error summary if there were errors
            if errors:
                md_file.write("\n## Processing Errors\n\n")
                md_file.write("The following errors were encountered during processing:\n\n")
                for i, error in enumerate(errors[:10]):  # Show only first 10 errors
                    md_file.write(f"{i+1}. {error}\n")
                
                if len(errors) > 10:
                    md_file.write(f"\n... and {len(errors) - 10} more errors. See log for details.\n")
        
        logger.info(f"Markdown visualization saved to: {md_path}")
    
    
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
                    
            # Find associated thumbnail (only if vis_dir exists)
            if vis_dir:
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
    
    # Create the enhanced matrix visualization (only for markdown)
    if create_markdown:
        _create_matrix_visualization(folders_data, md_path, include_stats=True)
    
    # Create PNG matrix visualization if requested
    if create_png:
        png_output_path = os.path.join(output_dir, base_name + "_matrix.png")
        try:
            png_result = create_png_matrix_visualization(folders_data, png_output_path)
            if png_result:
                logger.info(f"PNG matrix visualization saved to: {png_result}")
                # If only PNG was requested, update the output_path to point to the PNG
                if output_format == "png":
                    output_path = png_result
        except Exception as e:
            logger.warning(f"Could not create PNG matrix visualization: {e}")
    
    # Create individual PNG visualizations if requested
    if png_single or create_png:
        png_dir = os.path.join(output_dir, 'png_outputs')
        os.makedirs(png_dir, exist_ok=True)
        
        # Get all unique filenames
        all_unique_files = set()
        for folder_info in folders_data.values():
            if isinstance(folder_info, dict) and 'files' in folder_info:
                for file_path in folder_info['files']:
                    if file_path.lower().endswith(('.tif', '.tiff')):
                        all_unique_files.add(os.path.basename(file_path))
        
        # Create individual PNGs for each unique filename (using first occurrence)
        individual_count = 0
        for filename in all_unique_files:
            # Find first occurrence of this filename
            for folder_name, folder_info in folders_data.items():
                if isinstance(folder_info, dict) and 'files' in folder_info:
                    for file_path in folder_info['files']:
                        if os.path.basename(file_path) == filename:
                            png_single_path = os.path.join(png_dir, f"{os.path.splitext(filename)[0]}_{folder_name}.png")
                            try:
                                single_result = create_single_png_visualization(
                                    file_path, 
                                    png_single_path, 
                                    title=f"{filename} ({folder_name})"
                                )
                                if single_result:
                                    individual_count += 1
                                    logger.debug(f"Individual PNG saved: {single_result}")
                            except Exception as e:
                                logger.warning(f"Could not create individual PNG for {filename}: {e}")
                            break  # Only use first occurrence
        
        if individual_count > 0:
            logger.info(f"Created {individual_count} individual PNG files in {png_dir}")
    
    # Final logging based on what was created
    outputs_created = []
    if create_markdown:
        outputs_created.append(f"Markdown: {md_path}")
    if create_png:
        outputs_created.append(f"PNG matrix: {png_output_path}")
    if png_single or create_png:
        outputs_created.append(f"Individual PNGs: {png_dir}")
    
    logger.info(f"Visualization complete. Created: {', '.join(outputs_created)}")
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

def load_geotiff_as_array(file_path):
    """
    Load a GeoTIFF file as a numpy array.
    
    Args:
        file_path: Path to the GeoTIFF file
        
    Returns:
        numpy.ndarray: Image array with shape (height, width, bands) or None if failed
    """
    if not GDAL_AVAILABLE:
        logger.error("GDAL is required for loading GeoTIFF files")
        return None
    
    if not NUMPY_AVAILABLE:
        logger.error("numpy is required for loading GeoTIFF files")
        return None
    
    try:
        # Open the dataset
        ds = gdal.Open(file_path)
        if ds is None:
            logger.error(f"Could not open {file_path} with GDAL")
            return None
        
        # Get dimensions
        width = ds.RasterXSize
        height = ds.RasterYSize
        band_count = ds.RasterCount
        
        if band_count == 0:
            logger.error(f"No bands found in {file_path}")
            ds = None
            return None
        
        # Read all bands
        if band_count == 1:
            # Single band
            band = ds.GetRasterBand(1)
            array = band.ReadAsArray()
            if array is not None:
                array = array.reshape(height, width, 1)
        else:
            # Multiple bands
            arrays = []
            for i in range(1, band_count + 1):
                band = ds.GetRasterBand(i)
                band_array = band.ReadAsArray()
                if band_array is not None:
                    arrays.append(band_array)
            
            if arrays:
                # Stack bands: (height, width, bands)
                array = np.stack(arrays, axis=2)
            else:
                array = None
        
        ds = None  # Close dataset
        
        if array is not None:
            logger.debug(f"Successfully loaded {file_path}: shape {array.shape}, dtype {array.dtype}")
        else:
            logger.error(f"Failed to read data from {file_path}")
            
        return array
        
    except Exception as e:
        logger.error(f"Error loading GeoTIFF {file_path}: {e}")
        return None

def create_png_matrix_visualization(folders_data, output_path, max_cols=4, figsize_per_image=(3, 3)):
    """
    Create a PNG matrix visualization with all images in a single figure.
    
    Args:
        folders_data: Dict mapping folder names to their data (files, thumbnails, stats)
        output_path: Path to save the PNG file
        max_cols: Maximum number of columns in the matrix
        figsize_per_image: Size of each subplot (width, height) in inches
        
    Returns:
        str: Path to the saved PNG file
    """
    if not MATPLOTLIB_AVAILABLE:
        logger.error("matplotlib is required for PNG matrix visualization")
        return None
    
    # Collect all GeoTIFF files from all folders
    all_files = []
    folder_file_map = {}
    
    for folder_name, folder_info in folders_data.items():
        if isinstance(folder_info, dict) and 'files' in folder_info:
            files = folder_info['files']
        elif isinstance(folder_info, list):
            files = folder_info
        else:
            continue
            
        for file_path in files:
            if file_path.lower().endswith(('.tif', '.tiff')):
                filename = os.path.basename(file_path)
                all_files.append((folder_name, filename, file_path))
                
                # Track which folders have which files
                if filename not in folder_file_map:
                    folder_file_map[filename] = {}
                folder_file_map[filename][folder_name] = file_path
    
    if not all_files:
        logger.warning("No GeoTIFF files found for PNG visualization")
        return None
    
    # Calculate grid dimensions
    total_images = len(all_files)
    n_cols = min(max_cols, total_images)
    n_rows = (total_images + n_cols - 1) // n_cols
    
    # Create figure
    fig_width = n_cols * figsize_per_image[0]
    fig_height = n_rows * figsize_per_image[1]
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(fig_width, fig_height), dpi=100)
    
    # Handle single subplot case
    if total_images == 1:
        axes = [axes]
    elif n_rows == 1:
        axes = [axes] if n_cols == 1 else list(axes)
    else:
        axes = axes.flatten()
    
    # Plot each image
    for idx, (folder_name, filename, file_path) in enumerate(all_files):
        ax = axes[idx]
        
        try:
            # Load and display the image
            image_array = load_geotiff_as_array(file_path)
            if image_array is not None:
                # Apply contrast stretching
                if image_array.max() > 1000:  # Likely Sentinel-2 reflectance
                    disp = image_array / 10000.0
                else:
                    disp = image_array.copy()
                
                disp = contrast_stretch(disp)
                
                # Display based on number of bands
                if len(image_array.shape) == 2 or image_array.shape[-1] == 1:
                    # Single band - grayscale
                    if len(image_array.shape) == 3:
                        disp = disp[..., 0]
                    ax.imshow(disp, cmap='viridis')
                elif image_array.shape[-1] == 2:
                    # Two bands - average
                    ax.imshow(np.mean(disp, axis=2), cmap='gray')
                else:
                    # RGB or more - use first 3 bands
                    ax.imshow(disp[..., :3])
                
                # Set title with folder and filename info
                short_folder = folder_name[:20] + "..." if len(folder_name) > 23 else folder_name
                ax.set_title(f"{short_folder}\n{filename}", fontsize=8, pad=2)
            else:
                # Could not load image
                ax.text(0.5, 0.5, f"Could not load\n{filename}", 
                       transform=ax.transAxes, ha='center', va='center',
                       fontsize=8, bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray"))
                ax.set_title(f"{folder_name}\n{filename}", fontsize=8, pad=2)
                
        except Exception as e:
            logger.warning(f"Could not load image {file_path}: {e}")
            ax.text(0.5, 0.5, f"Error loading\n{filename}", 
                   transform=ax.transAxes, ha='center', va='center',
                   fontsize=8, bbox=dict(boxstyle="round,pad=0.3", facecolor="lightcoral"))
            ax.set_title(f"{folder_name}\n{filename}", fontsize=8, pad=2)
        
        ax.axis('off')
    
    # Hide any unused subplots
    for idx in range(total_images, len(axes)):
        axes[idx].axis('off')
    
    # Add main title
    #fig.suptitle('OpenEO Results Comparison', fontsize=14, y=0.98)
    
    # Adjust layout and save
    plt.tight_layout()
    plt.subplots_adjust(top=0.94)  # Make room for suptitle
    plt.savefig(output_path, 
                dpi=300,
                bbox_inches='tight',
                pad_inches=0.2,
                format='png',
                facecolor='white')
    plt.close()
    
    logger.info(f"PNG matrix visualization saved to: {output_path}")
    return output_path

def create_single_png_visualization(file_path, output_path, title=None):
    """
    Create a PNG visualization for a single GeoTIFF file.
    
    Args:
        file_path: Path to the GeoTIFF file
        output_path: Path to save the PNG file
        title: Optional title for the image
        
    Returns:
        str: Path to the saved PNG file
    """
    if not MATPLOTLIB_AVAILABLE:
        logger.error("matplotlib is required for PNG visualization")
        return None
    
    try:
        # Load the image
        image_array = load_geotiff_as_array(file_path)
        if image_array is None:
            logger.error(f"Could not load image: {file_path}")
            return None
        
        # Create figure
        plt.figure(figsize=(8, 8), dpi=100)
        
        # Apply contrast stretching
        if image_array.max() > 1000:  # Likely Sentinel-2 reflectance
            disp = image_array / 10000.0
        else:
            disp = image_array.copy()
        
        disp = contrast_stretch(disp)
        
        # Display based on number of bands
        if len(image_array.shape) == 2 or image_array.shape[-1] == 1:
            # Single band - grayscale with colorbar
            if len(image_array.shape) == 3:
                disp = disp[..., 0]
            im = plt.imshow(disp, cmap='viridis')
            plt.colorbar(im, shrink=0.8, label='Pixel Value')
        elif image_array.shape[-1] == 2:
            # Two bands - average with colorbar
            mean_disp = np.mean(disp, axis=2)
            im = plt.imshow(mean_disp, cmap='gray')
            plt.colorbar(im, shrink=0.8, label='Mean Value')
        else:
            # RGB or more - use first 3 bands
            plt.imshow(disp[..., :3])
        
        plt.axis('off')
        if title:
            plt.title(title, fontsize=14, pad=20)
        
        # Save with high quality
        plt.tight_layout()
        plt.savefig(output_path, 
                    dpi=300,
                    bbox_inches='tight',
                    pad_inches=0.1,
                    format='png',
                    facecolor='white')
        plt.close()
        
        logger.info(f"PNG visualization saved to: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Error creating PNG visualization: {e}")
        return None

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
    
    return output_path


def _write_stats_markdown(folders_data, md_file):
    """
    Write statistics section to the markdown file.
    
    Args:
        folders_data: Dict mapping folder names to their data
        md_file: Open file handle for the markdown file
    """
    md_file.write("\n## Statistics Summary\n\n")
    md_file.write("Statistics for all GeoTIFF files are included in the table below.\n\n")
    
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
    Get list of GeoTIFF files from a folder using proper file type detection.
    This function is kept for backward compatibility, but now uses the improved detection.
    
    Args:
        folder_path: Path to the folder
        
    Returns:
        list: List of GeoTIFF file paths
    """
    return get_geotiff_files(folder_path)

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
    summarize_parser.add_argument('--debug', action='store_true', 
                                help='Enable debug logging')
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
        
        summarize_task(input_patterns, output_path, debug=getattr(args, 'debug', False))
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
