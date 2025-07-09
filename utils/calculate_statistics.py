#!/usr/bin/env python3

import os
import csv
import argparse
import datetime
import statistics
from collections import defaultdict

def parse_date(date_str):
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")

def is_file_in_date_range(filename, start_date, end_date):
    """Check if the file is within the specified date range"""
    try:
        # Extract date from filename (assuming YYYY-MM-DD format at the beginning)
        date_str = filename[:10]  # Get the first 10 characters (YYYY-MM-DD)
        file_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        return start_date <= file_date <= end_date
    except (ValueError, IndexError):
        # If filename doesn't match expected format, skip it
        return False

def calculate_statistics(output_folder, start_date, end_date, output_file):
    """Calculate statistics from CSV files in the output folder within the date range"""
    # Dictionaries to store data for each backend and URL
    success_counts = defaultdict(lambda: defaultdict(int))
    total_counts = defaultdict(lambda: defaultdict(int))
    response_times = defaultdict(lambda: defaultdict(list))
    normalized_times = defaultdict(lambda: defaultdict(list))
    
    # Process each CSV file in the output folder
    for filename in os.listdir(output_folder):
        if not filename.endswith('.csv'):
            continue
            
        if not is_file_in_date_range(filename, start_date, end_date):
            continue
            
        file_path = os.path.join(output_folder, filename)
        
        try:
            with open(file_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=';')
                
                for row in reader:
                    backend = row['Backends']
                    url = row['url']
                    valid = row['Valid']
                    
                    # Count total and successful requests
                    total_counts[backend][url] += 1
                    if valid.lower() == 'true':
                        success_counts[backend][url] += 1
                    
                    # Collect response times for valid responses
                    try:
                        response_time = float(row['response_time'])
                        body_size = int(row['content_size'])
                        
                        if valid.lower() == 'true' and response_time is not None:
                            response_times[backend][url].append(response_time)
                            
                            # Calculate normalized response time (ms/byte)
                            if body_size > 0:  # Avoid division by zero
                                norm_time = response_time / body_size
                                normalized_times[backend][url].append(norm_time)
                    except (ValueError, TypeError):
                        # Skip if response time or body size is not a valid number
                        pass
                        
        except Exception as e:
            print(f"Error processing file {filename}: {str(e)}")
    
    # Calculate statistics and write to CSV file
    print(f"\nGenerating statistics for period: {start_date} to {end_date}")
    print(f"Writing results to: {output_file}")
    
    # Prepare data for CSV output
    csv_data = []
    
    for backend in sorted(total_counts.keys()):
        for url in sorted(total_counts[backend].keys()):
            total = total_counts[backend][url]
            success = success_counts[backend][url]
            success_ratio = (success / total * 100) if total > 0 else 0
            
            # Calculate average response time and std dev if available
            times = response_times[backend][url]
            avg_time = statistics.mean(times) if times else "N/A"
            std_dev = statistics.stdev(times) if times and len(times) > 1 else "N/A"
            
            # Calculate average normalized response time and std dev if available
            norm_times = normalized_times[backend][url]
            avg_norm = statistics.mean(norm_times) if norm_times else "N/A"
            norm_std_dev = statistics.stdev(norm_times) if norm_times and len(norm_times) > 1 else "N/A"
            
            # Add row to CSV data
            csv_data.append({
                'Backend': backend,
                'URL': url,
                'Success Ratio (%)': f"{success_ratio:.2f}",
                'Average Response Time (ms)': f"{avg_time:.2f}" if isinstance(avg_time, float) else avg_time,
                'Response Time StdDev (ms)': f"{std_dev:.2f}" if isinstance(std_dev, float) else std_dev,
                'Normalized Response Time (ms/byte)': f"{avg_norm:.6f}" if isinstance(avg_norm, float) else avg_norm,
                'Normalized Time StdDev (ms/byte)': f"{norm_std_dev:.6f}" if isinstance(norm_std_dev, float) else norm_std_dev
            })
    
    # Print statistics to console
    for row in csv_data:
        print(16*"=")
        print("Backend: " + row['Backend'])
        print("URL: " + row['URL'])
        print("Success Ratio: " + row['Success Ratio (%)'])
        print("Average Response Time: " + row['Average Response Time (ms)'])
        print("Response Time StdDev: " + row['Response Time StdDev (ms)'])
        print("Normalized Response Time: " + row['Normalized Response Time (ms/byte)'])
        print("Normalized Time StdDev: " + row['Normalized Time StdDev (ms/byte)'])

    print()
    # Write to CSV file
    if csv_data:
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['URL', 'Success Ratio (%)', 'Average Response Time (ms)', 'Response Time StdDev (ms)', 
                         'Normalized Response Time (ms/byte)', 'Normalized Time StdDev (ms/byte)']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_data)
        print(f"CSV file created successfully: {output_file}")
    else:
        print("No data to write to CSV file.")
    
    return {
        "success_counts": dict(success_counts),
        "total_counts": dict(total_counts),
        "response_times": dict(response_times),
        "normalized_times": dict(normalized_times)
    }

def main():
    parser = argparse.ArgumentParser(description='Calculate statistics from OpenEO-Checker output files')
    parser.add_argument('--folder', '-f', required=True, help='Folder containing the CSV output files')
    parser.add_argument('--start-date', '-s', type=parse_date, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', '-e', type=parse_date, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', '-o', required=True, help='Output CSV file to write results')
    
    args = parser.parse_args()
    
    # Validate that the output folder exists
    if not os.path.isdir(args.folder):
        print(f"Error: Output folder '{args.folder}' does not exist")
        return 1
    
    # Validate date range
    if args.end_date < args.start_date:
        print("Error: End date must be after start date")
        return 1
    
    # Calculate statistics and write to CSV
    calculate_statistics(args.folder, args.start_date, args.end_date, args.output)
    
    return 0

if __name__ == "__main__":
    exit(main())
