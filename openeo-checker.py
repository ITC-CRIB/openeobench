#!/usr/bin/env python3

import csv
import requests
import time
import argparse
import sys
import os
import datetime
from urllib.parse import urlparse, urljoin

def check_url(url):
    """
    Send a request to the URL and measure response time.
    Returns a tuple of (response_time, status, reason)
    """
    try:
        start_time = time.time()
        response = requests.get(url, timeout=30)
        end_time = time.time()
        response_time = round((end_time - start_time) * 1000, 2)  # Convert to ms and round to 2 decimal places
        return response_time, response.status_code, response.reason
    except requests.exceptions.Timeout:
        return None, "Timeout", "Request timed out"
    except requests.exceptions.RequestException as e:
        return None, "Request exception", str(e)

def process_csv(input_file, output_file):
    """
    Process the input CSV file and append results to the output CSV file.
    Treats URLs in the input as base URLs and appends predefined endpoints to each.
    Records HTTP response reasoning alongside the status code.
    Maintains a history of all test runs by appending new results to the output file.
    """
    # Define list of endpoints to test for each base URL
    endpoints = [
        "/",                 # Root path
        "/collections",      # OpenEO collections endpoint
        "/processes",        # OpenEO processes endpoint
    ]
    
    results = []
    # testing_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    testing_time = datetime.datetime.now().timestamp()
    
    try:
        # Check if output file exists and create it with headers if it doesn't
        file_exists = os.path.exists(output_file)
        
        # Read input file and process URLs
        with open(input_file, 'r') as infile:
            reader = csv.DictReader(infile)
            
            # Check if required columns exist
            if 'Backends' not in reader.fieldnames or 'URL' not in reader.fieldnames:
                print(f"Error: Input CSV must contain 'Backends' and 'URL' columns")
                sys.exit(1)
            
            # Process each URL and store results in memory
            try:
                for row in reader:
                    name = row['Backends']
                    base_url = row['URL'].rstrip('/')  # Remove trailing slash if present
                    
                    # Validate base URL
                    parsed_url = urlparse(base_url)
                    if not parsed_url.scheme or not parsed_url.netloc:
                        result_entry = {
                            'Backends': name,
                            'Base URL': base_url,
                            'Endpoint': '',
                            'Full URL': base_url,
                            'Response Time (ms)': None,
                            'Status': 'Invalid URL',
                            'Reason': 'Invalid URL format',
                            'Testing Time': testing_time
                        }
                        results.append(result_entry)
                        continue
                    
                    # Test each endpoint for this base URL
                    for endpoint in endpoints:
                        full_url = urljoin(base_url, endpoint)
                        print(f"Checking {name}: {full_url}")
                        response_time, status, reason = check_url(full_url)
                        
                        result_entry = {
                            'Backends': name,
                            'Base URL': base_url,
                            'Endpoint': endpoint,
                            'Full URL': full_url,
                            'Response Time (ms)': response_time,
                            'Status': status,
                            'Reason': reason,
                            'Testing Time': testing_time
                        }
                        results.append(result_entry)
                        
            except KeyboardInterrupt:
                print("\nProcess interrupted by user. Writing results collected so far...")
        
        # Define fieldnames for CSV
        fieldnames = ['Backends', 'Base URL', 'Endpoint', 'Full URL', 'Response Time (ms)', 'Status', 'Reason', 'Testing Time']
        
        # Write results to output file - either create new file or append to existing
        mode = 'a' if file_exists else 'w'
        with open(output_file, mode, newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=';')
            
            # Write header only if creating a new file
            if not file_exists:
                writer.writeheader()
            
            # Write new results
            for result in results:
                writer.writerow(result)
                
        print(f"Results appended to {output_file}")
        
    except FileNotFoundError:
        print(f"Error: Could not find input file {input_file}")
        sys.exit(1)
    except PermissionError:
        print(f"Error: Permission denied when accessing files")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Check URLs from a CSV file and output response times and status')
    parser.add_argument('-i', '--input', required=True, help='Input CSV file with Backends and URL columns')
    parser.add_argument('-o', '--output', required=True, help='Output CSV file to write results')
    
    args = parser.parse_args()
    
    process_csv(args.input, args.output)

if __name__ == "__main__":
    main()