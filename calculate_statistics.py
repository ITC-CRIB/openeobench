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

def calculate_statistics(output_folder, start_date, end_date):
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
                    url = row['URL']
                    valid = row['Valid']
                    
                    # Count total and successful requests
                    total_counts[backend][url] += 1
                    if valid.lower() == 'true':
                        success_counts[backend][url] += 1
                    
                    # Collect response times for valid responses
                    try:
                        response_time = float(row['Response Time (ms)'])
                        body_size = int(row['Body Size (bytes)'])
                        
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
    
    # Calculate and print statistics
    print(f"\nStatistics for period: {start_date} to {end_date}")
    print("=" * 80)
    
    for backend in sorted(total_counts.keys()):
        print(f"\nBackend: {backend}")
        print("-" * 40)
        
        for url in sorted(total_counts[backend].keys()):
            total = total_counts[backend][url]
            success = success_counts[backend][url]
            success_ratio = (success / total * 100) if total > 0 else 0
            
            print(f"URL: {url}")
            print(f"  Success ratio: {success_ratio:.2f}% ({success}/{total})")
            
            # Calculate response time statistics if available
            times = response_times[backend][url]
            if times:
                avg_time = statistics.mean(times)
                try:
                    std_dev = statistics.stdev(times) if len(times) > 1 else 0
                    print(f"  Avg response time: {avg_time:.2f} ± {std_dev:.2f} ms")
                except statistics.StatisticsError:
                    print(f"  Avg response time: {avg_time:.2f} ms (std dev not available)")
                    
                # Calculate normalized response time statistics
                norm_times = normalized_times[backend][url]
                if norm_times:
                    avg_norm = statistics.mean(norm_times)
                    try:
                        norm_std_dev = statistics.stdev(norm_times) if len(norm_times) > 1 else 0
                        print(f"  Normalized response time: {avg_norm:.6f} ± {norm_std_dev:.6f} ms/byte")
                    except statistics.StatisticsError:
                        print(f"  Normalized response time: {avg_norm:.6f} ms/byte (std dev not available)")
                else:
                    print("  No valid normalized response times available")
            else:
                print("  No valid response times available")
            
            print()
    
    return {
        "success_counts": dict(success_counts),
        "total_counts": dict(total_counts),
        "response_times": dict(response_times),
        "normalized_times": dict(normalized_times)
    }

def main():
    parser = argparse.ArgumentParser(description='Calculate statistics from OpenEO-Checker output files')
    parser.add_argument('--output-folder', '-o', required=True, help='Folder containing the CSV output files')
    parser.add_argument('--start-date', '-s', type=parse_date, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', '-e', type=parse_date, required=True, help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Validate that the output folder exists
    if not os.path.isdir(args.output_folder):
        print(f"Error: Output folder '{args.output_folder}' does not exist")
        return 1
    
    # Validate date range
    if args.end_date < args.start_date:
        print("Error: End date must be after start date")
        return 1
    
    # Calculate statistics
    calculate_statistics(args.output_folder, args.start_date, args.end_date)
    
    return 0

if __name__ == "__main__":
    exit(main())
