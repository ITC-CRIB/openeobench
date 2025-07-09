#!/usr/bin/env python3

import argparse
import csv
import datetime
import json
import os
import statistics
import sys
import time
from collections import defaultdict
from urllib.parse import urlparse

import requests


def parse_json_content(content):
    try:
        json_content = json.loads(content)
        return True, json_content
    except json.JSONDecodeError:
        return False, None


def check_url(url):
    """
    Send a request to the URL and measure response time.
    Returns a tuple of (response_time, status, reason)
    """
    valid = False
    try:
        start_time = time.time()
        response = requests.get(url, timeout=30)
        end_time = time.time()
        response_time = round(
            (end_time - start_time) * 1000, 2
        )  # Convert to ms and round to 2 decimal places
        # Try to parse response content as JSON
        is_json, json_content = parse_json_content(response.content)
        # Default reason is from response reason
        reason = response.reason
        # Check if response status code is between 100 and 399
        if response.status_code >= 100 and response.status_code <= 399:
            # If JSON is valid, mark as valid
            if is_json:
                valid = True
        else:
            valid = False
            # If JSON is valid, get message from JSON
            if is_json:
                # Check if there is a message in the JSON content
                if "message" in json_content:
                    reason = json_content["message"]
        body_size = len(response.content)
        return response_time, response.status_code, reason, valid, body_size
    except requests.exceptions.Timeout:
        return None, "Timeout", "Request timed out", False, 0
    except requests.exceptions.RequestException as e:
        return None, "Request exception", str(e), False, 0


def process_single_url(url, backend_name, output_file):
    """
    Process a single URL and append result to the output CSV file.
    """
    results = []
    testing_time = datetime.datetime.now().timestamp()

    try:
        # Check if output file exists and create it with headers if it doesn't
        file_exists = os.path.exists(output_file)

        # If no backend name provided, use hostname
        if not backend_name:
            parsed_url = urlparse(url)
            backend_name = parsed_url.netloc if parsed_url.netloc else "unknown"

        # Validate URL
        parsed_url = urlparse(url)
        if not parsed_url.scheme or not parsed_url.netloc:
            result_entry = {
                "URL": url,
                "Timestamp": testing_time,
                "Response Time (ms)": None,
                "HTTP Code": "Invalid URL",
                "Errors": "Invalid URL format",
                "Body Size (bytes)": 0,
            }
            results.append(result_entry)
        else:
            print(f"Checking {backend_name}: {url}")
            response_time, status, reason, valid, body_size = check_url(url)

            result_entry = {
                "URL": url,
                "Timestamp": testing_time,
                "Response Time (ms)": response_time,
                "HTTP Code": status,
                "Errors": reason,
                "Body Size (bytes)": body_size,
            }
            results.append(result_entry)

        # Define fieldnames for CSV
        fieldnames = [
            "URL",
            "Timestamp",
            "Response Time (ms)",
            "HTTP Code",
            "Errors",
            "Body Size (bytes)",
        ]

        # Write results to output file - either create new file or append to existing
        mode = "a" if file_exists else "w"
        with open(output_file, mode, newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=";")

            # Write header only if creating a new file
            if not file_exists:
                writer.writeheader()

            # Write new results
            for result in results:
                writer.writerow(result)

        print(f"Results appended to {output_file}")

        # Print immediate results to console
        print("\nResults:")
        for result in results:
            print(f"URL: {result['URL']}")
            print(f"Response Time: {result['Response Time (ms)']} ms")
            print(f"HTTP Code: {result['HTTP Code']}")
            print(f"Errors: {result['Errors']}")
            print(f"Body Size: {result['Body Size (bytes)']} bytes")

    except PermissionError:
        print("Error: Permission denied when accessing files")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


def process_csv(input_file, output_file):
    """
    Process the input CSV file and append results to the output CSV file.
    Treats URLs in the input as base URLs and appends predefined endpoints to each.
    Records HTTP response reasoning alongside the status code.
    Maintains a history of all test runs by appending new results to the output file.
    """

    results = []
    # testing_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    testing_time = datetime.datetime.now().timestamp()

    try:
        # Check if output file exists and create it with headers if it doesn't
        file_exists = os.path.exists(output_file)

        # Read input file and process URLs
        with open(input_file, "r") as infile:
            reader = csv.DictReader(infile)

            # Check if required columns exist
            if "URL" not in reader.fieldnames:
                print("Error: Input CSV must contain 'URL' column")
                sys.exit(1)

            # Process each URL and store results in memory
            try:
                for row in reader:
                    # Use Backends column if it exists, otherwise use hostname
                    if "Backends" in reader.fieldnames and row["Backends"].strip():
                        name = row["Backends"].strip()
                    else:
                        # Extract hostname from URL
                        parsed_url = urlparse(row["URL"].strip())
                        name = parsed_url.netloc if parsed_url.netloc else "unknown"

                    base_url = (
                        row["URL"].strip().rstrip("/")
                    )  # Remove trailing slash if present

                    # Validate base URL
                    parsed_url = urlparse(base_url)
                    if not parsed_url.scheme or not parsed_url.netloc:
                        result_entry = {
                            "URL": base_url,
                            "Timestamp": testing_time,
                            "Response Time (ms)": None,
                            "HTTP Code": "Invalid URL",
                            "Errors": "Invalid URL format",
                            "Body Size (bytes)": 0,
                        }
                        results.append(result_entry)
                        continue

                    print(f"Checking {name}: {base_url}")
                    response_time, status, reason, valid, body_size = check_url(
                        base_url
                    )

                    result_entry = {
                        "URL": base_url,
                        "Timestamp": testing_time,
                        "Response Time (ms)": response_time,
                        "HTTP Code": status,
                        "Errors": reason,
                        "Body Size (bytes)": body_size,
                    }
                    results.append(result_entry)

            except KeyboardInterrupt:
                print(
                    "\nProcess interrupted by user. Writing results collected so far..."
                )

        # Define fieldnames for CSV
        fieldnames = [
            "URL",
            "Timestamp",
            "Response Time (ms)",
            "HTTP Code",
            "Errors",
            "Body Size (bytes)",
        ]

        # Write results to output file - either create new file or append to existing
        mode = "a" if file_exists else "w"
        with open(output_file, mode, newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=";")

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
        print("Error: Permission denied when accessing files")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


def parse_date(date_str):
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD"
        )


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


def calculate_statistics_from_files(output_folder, start_date, end_date, output_file):
    """Calculate statistics from CSV files in the output folder within the date range"""
    # Dictionaries to store data for each URL
    success_counts = defaultdict(int)
    total_counts = defaultdict(int)
    response_times = defaultdict(list)
    normalized_times = defaultdict(list)

    # Process each CSV file in the output folder
    for filename in os.listdir(output_folder):
        if not filename.endswith(".csv"):
            continue

        if not is_file_in_date_range(filename, start_date, end_date):
            continue

        file_path = os.path.join(output_folder, filename)

        try:
            with open(file_path, "r") as csvfile:
                reader = csv.DictReader(csvfile, delimiter=";")

                for row in reader:
                    url = row["URL"]
                    http_code = row["HTTP Code"]

                    # Count total requests
                    total_counts[url] += 1

                    # Count successful requests (HTTP 200 codes)
                    if http_code == "200":
                        success_counts[url] += 1

                    # Collect response times for successful responses
                    try:
                        response_time = float(row["Response Time (ms)"])
                        body_size = int(row["Body Size (bytes)"])

                        if http_code == "200" and response_time is not None:
                            response_times[url].append(response_time)

                            # Calculate normalized response time (ms/Kbyte)
                            if body_size > 0:  # Avoid division by zero
                                norm_time = response_time / (body_size / 1024)
                                normalized_times[url].append(norm_time)
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

    for url in sorted(total_counts.keys()):
        total = total_counts[url]
        success = success_counts[url]
        success_ratio = (success / total * 100) if total > 0 else 0

        # Calculate average response time and std dev if available
        times = response_times[url]
        avg_time = statistics.mean(times) if times else "N/A"
        std_dev = statistics.stdev(times) if times and len(times) > 1 else "N/A"

        # Calculate average normalized response time and std dev if available
        norm_times = normalized_times[url]
        avg_norm = statistics.mean(norm_times) if norm_times else "N/A"
        norm_std_dev = (
            statistics.stdev(norm_times)
            if norm_times and len(norm_times) > 1
            else "N/A"
        )

        # Add row to CSV data
        csv_data.append(
            {
                "URL": url,
                "Success Ratio (%)": f"{success_ratio:.2f}",
                "Average Response Time (ms)": f"{avg_time:.2f}"
                if isinstance(avg_time, float)
                else avg_time,
                "Response Time StdDev (ms)": f"{std_dev:.2f}"
                if isinstance(std_dev, float)
                else std_dev,
                "Normalized Response Time (ms/Kbyte)": f"{avg_norm:.6f}"
                if isinstance(avg_norm, float)
                else avg_norm,
                "Normalized Time StdDev (ms/Kbyte)": f"{norm_std_dev:.6f}"
                if isinstance(norm_std_dev, float)
                else norm_std_dev,
            }
        )

    # Print statistics to console
    for row in csv_data:
        print(16 * "=")
        print("URL: " + row["URL"])
        print("Success Ratio: " + row["Success Ratio (%)"])
        print("Average Response Time: " + row["Average Response Time (ms)"])
        print("Response Time StdDev: " + row["Response Time StdDev (ms)"])
        print("Normalized Response Time: " + row["Normalized Response Time (ms/Kbyte)"])
        print("Normalized Time StdDev: " + row["Normalized Time StdDev (ms/Kbyte)"])

    print()
    # Write to CSV file
    if csv_data:
        with open(output_file, "w", newline="") as csvfile:
            fieldnames = [
                "URL",
                "Success Ratio (%)",
                "Average Response Time (ms)",
                "Response Time StdDev (ms)",
                "Normalized Response Time (ms/Kbyte)",
                "Normalized Time StdDev (ms/Kbyte)",
            ]
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
        "normalized_times": dict(normalized_times),
    }


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Check URLs and calculate statistics for OpenEO endpoints"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Check command
    check_parser = subparsers.add_parser(
        "check", help="Check URLs for availability and response times"
    )
    check_group = check_parser.add_mutually_exclusive_group(required=True)
    check_group.add_argument("-i", "--input", help="Input CSV file with URL column")
    check_group.add_argument("-u", "--url", help="Single URL to test")
    check_parser.add_argument(
        "-o", "--output", required=True, help="Output directory to write results"
    )
    check_parser.add_argument(
        "-n",
        "--name",
        help="Backend name for single URL (optional, defaults to hostname)",
    )

    # Statistics command
    stats_parser = subparsers.add_parser(
        "stats", help="Calculate statistics from previous check results"
    )
    stats_parser.add_argument(
        "--folder", "-f", required=True, help="Folder containing the CSV output files"
    )
    stats_parser.add_argument(
        "--start-date",
        "-s",
        type=parse_date,
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    stats_parser.add_argument(
        "--end-date", "-e", type=parse_date, required=True, help="End date (YYYY-MM-DD)"
    )
    stats_parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Output CSV file to write statistics results",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "check":
        # Check if output directory exists, create it if it doesn't
        output_dir = args.output
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Get date in YYYY-MM-DD format
        date = datetime.datetime.now().strftime("%Y-%m-%d")

        if args.url:
            # Single URL mode
            output_csv = os.path.join(output_dir, f"{date}_single.csv")
            process_single_url(args.url, args.name, output_csv)
        else:
            # CSV file mode
            output_csv = os.path.join(output_dir, f"{date}.csv")
            process_csv(args.input, output_csv)

    elif args.command == "stats":
        # Validate that the output folder exists
        if not os.path.isdir(args.folder):
            print(f"Error: Output folder '{args.folder}' does not exist")
            return 1

        # Validate date range
        if args.end_date < args.start_date:
            print("Error: End date must be after start date")
            return 1

        # Calculate statistics and write to CSV
        calculate_statistics_from_files(
            args.folder, args.start_date, args.end_date, args.output
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
