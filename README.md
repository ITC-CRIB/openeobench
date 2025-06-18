# OpenEO Checker

A simple Python utility for checking the availability and response times of OpenEO API endpoints.

## Description

OpenEO Checker is a command-line tool that reads a CSV file containing OpenEO backend names and their API endpoints, tests each endpoint's availability, and outputs the results to a CSV file with response times and status codes.

## Features

- Validates URLs before sending requests
- Measures response time in milliseconds
- Handles various error conditions (timeouts, connection errors)
- Gracefully handles interruptions, saving partial results
- Provides clear error messages and status codes

## Requirements

- Python 3.x
- `requests` library

## Installation

1. Clone this repository or download the script files
2. Install the required dependencies:

```bash
pip install requests
```

## Usage

```bash
python openeo-checker.py -i <input_csv_file> -o <output_directory>
```

### Arguments

- `-i, --input`: Path to the input CSV file (required)
- `-o, --output`: Path to the output directory (required)

### Input CSV Format

The input CSV file must contain at least two columns:
- `Backends`: Name of the OpenEO backend
- `URL`: URL of the API endpoint to check

Example:
```csv
Backends,URL
copernicus-dataspace,https://openeo.dataspace.copernicus.eu/.well-known/openeo
eodc,https://openeo.eodc.eu/.well-known/openeo
```

### Output CSV Format

The output CSV file will contain the following columns (semicolon-delimited):
- `Backends`: Name of the OpenEO backend
- `URL`: URL of the API endpoint
- `Timestamp`: Unix timestamp of when the test was run
- `Response Time (ms)`: Response time in milliseconds (if successful)
- `HTTP Code`: HTTP status code or error message
- `Reason`: Reason phrase for the HTTP status or error description
- `Valid`: Boolean indicating if the endpoint returned a valid JSON response with a success status code
- `Body Size (bytes)`: Size of the response body in bytes

## Error Handling

The script handles various error conditions:
- Invalid URLs: Marked as "Invalid URL" in the HTTP Code column with "Invalid URL format" in the Reason column
- Timeouts: Marked as "Timeout" in the HTTP Code column with "Request timed out" in the Reason column
- Connection errors: Marked as "Request exception" in the HTTP Code column with the specific error message in the Reason column
- For HTTP error responses (4xx, 5xx), the script attempts to extract error messages from JSON responses when available

## Example

```bash
python openeo-checker.py -i api-endpoint.csv -o output_directory
```

This will create a CSV file in the specified output directory with the current date as part of the filename (e.g., `2025-06-18_OpenEO-Checker.csv`).

## License

MIT License

