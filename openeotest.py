#!/usr/bin/env python3
"""
openeotest.py - OpenEO Backend Scenario Runner and Summarizer

This script provides two main functionalities:
1. Run: Connect to an OpenEO backend, authenticate, and execute a batch job for a specified process graph
2. Summarize: Generate summary reports from output folders in either markdown or CSV format
"""

import os
import json
import logging
import glob
from pathlib import Path
import openeo
import sys
import datetime
import argparse
import urllib.parse
import shutil
import csv
import time

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
        job_start_datetime = datetime.datetime.now()
        
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
            job_description = job.describe()
            job_description_file = os.path.join(output_directory, "job-description.json")
            try:
                with open(job_description_file, 'w', encoding='utf-8') as f:
                    json.dump(job_description, f, indent=2)
                logger.info(f"Saved job description to {job_description_file}")
            except Exception as e:
                logger.warning(f"Failed to save job job decription: {e}")
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
        }
        
        with open(os.path.join(output_directory, 'job_logs.json'), 'w') as f:
            json.dump(legacy_logs, f, indent=2)
        '''    
    except Exception as e:
        logger.warning(f"Failed to save results: {e}")


def summarize_task(input_folders, output_format):
    """Generate a summary report from output folders."""
    logger.info(f"Summarizing results from {len(input_folders)} folders in {output_format} format")

def generate_markdown_summary(results):
    """Generate a markdown summary report."""
    logger.info("Generating markdown summary report")

def generate_csv_summary(results):
    """Generate a CSV summary report."""
    logger.info("Generating CSV summary report")


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
    summarize_parser.add_argument('--input', required=True, nargs='+', help='Input folders to summarize')
    summarize_parser.add_argument('--output', required=True, choices=['md', 'csv'], help='Output format (md or csv)')
    
    args = parser.parse_args()
    
    if args.task == 'run':
        run_task(args.api_url, args.scenario, args.output_directory)
    elif args.task == 'summarize':
        summarize_task(args.input, args.output)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
