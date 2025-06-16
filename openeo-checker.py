#!/usr/bin/env python3

import csv
import requests
import time
import argparse
import sys
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
    Process the input CSV file and write results to the output CSV file.
    Only writes output after all URLs have been checked or if interrupted.
    """
    results = []
    
    try:
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
                        results.append({
                            'Backends': name,
                            'URL': url,
                            'Response Time (ms)': None,
                            'Status': 'Invalid URL'
                        })
                        continue
                        
                    print(f"Checking {name}: {url}")
                    response_time, status = check_url(url)
                    
                    results.append({
                        'Backends': name,
                        'URL': url,
                        'Response Time (ms)': response_time,
                        'Status': status
                    })
            except KeyboardInterrupt:
                print("\nProcess interrupted by user. Writing results collected so far...")
        
        # Write all results to output file
        with open(output_file, 'w', newline='') as outfile:
            fieldnames = ['Backends', 'URL', 'Response Time (ms)', 'Status']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                writer.writerow(result)
                
        print(f"Results written to {output_file}")
        
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