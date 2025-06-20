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
        connection = openeo.connect(backend['url'])
        logger.info(f"Connected to backend: {backend['name']} at {backend['url']}")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to backend {backend['name']} at {backend['url']}: {str(e)}")
        return None

def authenticate(connection, backend_name):
    """Authenticate with the backend."""
    try:
        # Try basic authentication if credentials are provided in environment variables
        # username = os.environ.get(f"{backend_name.upper()}_USERNAME")
        # password = os.environ.get(f"{backend_name.upper()}_PASSWORD")
        
        # if username and password:
        #     connection.authenticate_oidc()
        #     logger.info(f"Authenticated with backend {backend_name} using basic auth")
        # else:
        #     # If no credentials, try guest access
        #     logger.info(f"No credentials found for {backend_name}, proceeding with guest access")

        connection.authenticate_oidc()
        
        return True
    except Exception as e:
        logger.error(f"Authentication failed for backend {backend_name}: {str(e)}")
        return False

def execute_batch_job(connection, process_graph, pg_name, backend_name, scenario_output_dir):
    """Execute a batch job with the given process graph."""
    try:
        # Create a batch job
        job = connection.create_job(process_graph)
        job_id = job.job_id
        
        logger.info(f"Created batch job {job_id} for process graph {pg_name} on backend {backend_name}")
        
        # Start the job
        
        logger.info(f"Started batch job {job_id}")
        job.start_and_wait()

        logger.info(f"Downloaded results for job {job_id}")
        job.get_results().download_files(scenario_output_dir)
        
        # Wait for job completion (with timeout)
        status = job.status()
        logger.info(f"Job {job_id} status: {status}")
        
        return job_id, status
    except Exception as e:
        logger.error(f"Failed to execute batch job for process graph {pg_name} on backend {backend_name}: {str(e)}")
        return None, "failed"

def run_task(api_url, scenario_path, output_directory=None):
    """Run a specific scenario on a given backend."""
    # Parse hostname for default output directory
    hostname = urllib.parse.urlparse(api_url).netloc
    scenario_name = os.path.basename(scenario_path).replace('.json', '')
    date_time = datetime.datetime.now().strftime("%Y%m%d%H%M")
    
    # Set default output directory if not provided
    if not output_directory:
        output_directory = f"{hostname.replace('.', '_')}_{scenario_name}_{date_time}"
        output_directory = os.path.join("output", output_directory)
    
    # Create output directory
    os.makedirs(output_directory, exist_ok=True)

    # Copy scenario file to output directory
    shutil.copyfile(scenario_path, os.path.join(output_directory, "processgraph.json"))
    
    logger.info(f"Processing backend at {api_url} with scenario {scenario_name}")
    
    # Load the process graph
    try:
        with open(scenario_path, 'r') as f:
            process_graph = json.load(f)
        logger.info(f"Loaded process graph from {scenario_path}")
    except Exception as e:
        logger.error(f"Failed to load process graph from {scenario_path}: {str(e)}")
        return
    
    # Connect to backend
    try:
        connection = openeo.connect(api_url)
        logger.info(f"Connected to backend at {api_url}")
    except Exception as e:
        logger.error(f"Failed to connect to backend at {api_url}: {str(e)}")
        return
    
    # Authenticate
    try:
        connection.authenticate_oidc()
        logger.info(f"Authenticated with backend")
    except Exception as e:
        logger.error(f"Authentication failed for backend: {str(e)}")
        return
    
    # Execute batch job
    try:
        # Create a batch job
        job = connection.create_job(process_graph)
        job_id = job.job_id
        
        logger.info(f"Created batch job {job_id} for process graph {scenario_name}")
        
        # Start the job and wait for completion
        logger.info(f"Started batch job {job_id}")
        job.start_and_wait()

        # Download results
        logger.info(f"Downloading results for job {job_id}")
        job.get_results().download_files(output_directory)
        
        # Get final status
        status = job.status()
        logger.info(f"Job {job_id} status: {status}")
        
        # Save job details
        result = {
            'backend_url': api_url,
            'process_graph': scenario_name,
            'job_id': job_id,
            'status': status
        }
        
        # Save result to file
        with open(os.path.join(output_directory, 'job_result.json'), 'w') as f:
            json.dump(result, f, indent=2)
        
        logger.info(f"Run completed. Results saved to {output_directory}")
        
    except Exception as e:
        logger.error(f"Failed to execute batch job for process graph {scenario_name}: {str(e)}")


def summarize_task(input_folders, output_format):
    """Generate a summary report from output folders."""
    logger.info(f"Summarizing results from {len(input_folders)} folders in {output_format} format")
    
    # Collect results from all input folders
    all_results = []
    
    for folder in input_folders:
        result_file = os.path.join(folder, 'job_result.json')
        if os.path.exists(result_file):
            try:
                with open(result_file, 'r') as f:
                    result = json.load(f)
                
                # Add folder path to the result
                result['output_folder'] = folder
                
                # Check if there are actual output files
                output_files = [f for f in os.listdir(folder) if f != 'job_result.json']
                result['output_files'] = output_files
                result['output_file_count'] = len(output_files)
                
                all_results.append(result)
                logger.info(f"Added results from {folder}")
            except Exception as e:
                logger.error(f"Failed to load results from {folder}: {str(e)}")
        else:
            logger.warning(f"No result file found in {folder}")
    
    if not all_results:
        logger.error("No results found in any of the input folders")
        return
    
    # Generate output based on format
    if output_format.lower() == 'md':
        generate_markdown_summary(all_results)
    elif output_format.lower() == 'csv':
        generate_csv_summary(all_results)
    else:
        logger.error(f"Unsupported output format: {output_format}. Use 'md' or 'csv'.")


def generate_markdown_summary(results):
    """Generate a markdown summary report."""
    output_file = f"summary_{datetime.datetime.now().strftime('%Y%m%d%H%M')}.md"
    
    with open(output_file, 'w') as f:
        f.write("# OpenEO Test Summary Report\n\n")
        f.write(f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        
        f.write("## Overview\n\n")
        f.write(f"Total tests: {len(results)}\n\n")
        
        # Count successful and failed jobs
        successful = sum(1 for r in results if r.get('status') == 'finished')
        failed = len(results) - successful
        
        f.write(f"- Successful: {successful}\n")
        f.write(f"- Failed: {failed}\n\n")
        
        f.write("## Detailed Results\n\n")
        f.write("| Backend | Process Graph | Status | Output Files | Job ID |\n")
        f.write("|---------|---------------|--------|--------------|--------|\n")
        
        for result in results:
            backend = result.get('backend_url', 'N/A')
            pg_name = result.get('process_graph', 'N/A')
            status = result.get('status', 'N/A')
            file_count = result.get('output_file_count', 0)
            job_id = result.get('job_id', 'N/A')
            
            f.write(f"| {backend} | {pg_name} | {status} | {file_count} | {job_id} |\n")
    
    logger.info(f"Markdown summary generated: {output_file}")


def generate_csv_summary(results):
    """Generate a CSV summary report."""
    output_file = f"summary_{datetime.datetime.now().strftime('%Y%m%d%H%M')}.csv"
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow(['Backend', 'Process Graph', 'Status', 'Output Files', 'Job ID', 'Output Folder'])
        
        # Write data rows
        for result in results:
            writer.writerow([
                result.get('backend_url', 'N/A'),
                result.get('process_graph', 'N/A'),
                result.get('status', 'N/A'),
                result.get('output_file_count', 0),
                result.get('job_id', 'N/A'),
                result.get('output_folder', 'N/A')
            ])
    
    logger.info(f"CSV summary generated: {output_file}")


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
