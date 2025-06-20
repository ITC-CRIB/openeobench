#!/usr/bin/env python3
"""
scenario_runner.py - OpenEO Backend Scenario Runner

This script iterates through backends defined in backends.json, connects to each backend,
authenticates, and executes batch jobs for all process graphs in the process_graph folder.
"""

import os
import json
import logging
import glob
from pathlib import Path
import openeo
import sys
import datetime

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

def main():
    """Main function to run scenarios on all backends."""
    # Load backends
    backends = load_backends('backends.json')
    if not backends:
        logger.error("No backends found. Exiting.")
        return
    
    # Load process graphs
    process_graphs = load_process_graphs('process_graph')
    if not process_graphs:
        logger.warning("No process graphs found. Please add JSON process graphs to the process_graph directory.")
        return
    
    # Track results
    results = []

    date_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Process each backend
    for backend in backends:
        scenario_output_dir = os.path.join("output", date_time, backend['name'])
        os.makedirs(scenario_output_dir, exist_ok=True)

        logger.info(f"Processing backend: {backend['name']}")
        
        # Connect to backend
        connection = connect_to_backend(backend)
        if not connection:
            continue
        
        # Authenticate
        if not authenticate(connection, backend['name']):
            continue
        
        # Execute batch jobs for each process graph
        for pg_name, process_graph in process_graphs.items():
            logger.info(f"Executing process graph {pg_name} on backend {backend['name']}")
            job_id, status = execute_batch_job(connection, process_graph, pg_name, backend['name'], scenario_output_dir)
            
            results.append({
                'backend': backend['name'],
                'process_graph': pg_name,
                'job_id': job_id,
                'status': status
            })
    
    # Save results to file
    with open(os.path.join(scenario_output_dir, 'scenario_results.json'), 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"Scenario run completed. Results saved to scenario_results.json")

if __name__ == "__main__":
    main()
