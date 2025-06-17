#!/usr/bin/env python3

import csv
import requests
import time
import argparse
import sys
import os
import datetime
from urllib.parse import urlparse

def check_url(url):
    """
    Send a request to the URL and measure response time.
    Returns a tuple of (response_time, status)
    """
    try:
        start_time = time.time()
        response = requests.get(url, timeout=30)
        end_time = time.time()
        response_time = round((end_time - start_time) * 1000, 2)  # Convert to ms and round to 2 decimal places
        return response_time, response.status_code
    except requests.exceptions.Timeout:
        return None, "Timeout"
    except requests.exceptions.ConnectionError:
        return None, "Connection Error"
    except requests.exceptions.RequestException as e:
        return None, f"Error: {str(e)}"

def process_csv(input_file, output_file):
    """
    Process the input CSV file and append results to the output CSV file.
    Only writes output after all URLs have been checked or if interrupted.
    Records previous test results and includes testing time in the output.
    Maintains a history of all test runs by appending new results to the output file.
    """
    results = []
    previous_results = {}
    existing_entries = []
    testing_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        # Load previous results if output file exists
        if os.path.exists(output_file):
            try:
                with open(output_file, 'r') as prev_file:
                    prev_reader = csv.DictReader(prev_file)
                    # Store all existing entries to preserve them
                    existing_entries = list(prev_reader)
                    
                    # Get the most recent results for each backend/URL combination
                    for row in existing_entries:
                        if 'Backends' in row and 'URL' in row:
                            key = f"{row['Backends']}:{row['URL']}"
                            # Only update if this is a newer entry than what we have
                            if key not in previous_results or ('Testing Time' in row and row['Testing Time']):
                                previous_results[key] = {
                                    'Response Time (ms)': row.get('Response Time (ms)'),
                                    'Status': row.get('Status')
                                }
            except Exception as e:
                print(f"Warning: Could not read previous results: {str(e)}")
        
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
                    url = row['URL']
                    
                    # Validate URL
                    parsed_url = urlparse(url)
                    if not parsed_url.scheme or not parsed_url.netloc:
                        result_entry = {
                            'Backends': name,
                            'URL': url,
                            'Response Time (ms)': None,
                            'Status': 'Invalid URL',
                            'Testing Time': testing_time
                        }
                        
                        # Add previous results if available
                        key = f"{name}:{url}"
                        if key in previous_results:
                            result_entry['Previous Response Time (ms)'] = previous_results[key]['Response Time (ms)']
                            result_entry['Previous Status'] = previous_results[key]['Status']
                            
                        results.append(result_entry)
                        continue
                        
                    print(f"Checking {name}: {url}")
                    response_time, status = check_url(url)
                    
                    result_entry = {
                        'Backends': name,
                        'URL': url,
                        'Response Time (ms)': response_time,
                        'Status': status,
                        'Testing Time': testing_time
                    }
                    
                    # Add previous results if available
                    key = f"{name}:{url}"
                    if key in previous_results:
                        result_entry['Previous Response Time (ms)'] = previous_results[key]['Response Time (ms)']
                        result_entry['Previous Status'] = previous_results[key]['Status']
                        
                    results.append(result_entry)
            except KeyboardInterrupt:
                print("\nProcess interrupted by user. Writing results collected so far...")
        
        # Write all results to output file (append mode)
        file_exists = os.path.exists(output_file) and os.path.getsize(output_file) > 0
        
        with open(output_file, 'w', newline='') as outfile:
            fieldnames = ['Backends', 'URL', 'Response Time (ms)', 'Status', 'Testing Time', 
                         'Previous Response Time (ms)', 'Previous Status']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # First write all existing entries to preserve history
            for entry in existing_entries:
                # Ensure all entries have the same fields
                row_data = {field: entry.get(field, '') for field in fieldnames}
                writer.writerow(row_data)
            
            # Then write new results
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