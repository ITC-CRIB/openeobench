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
python openeo-checker.py -i <input_csv_file> -o <output_csv_file>
```

### Arguments

- `-i, --input`: Path to the input CSV file (required)
- `-o, --output`: Path to the output CSV file (required)

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

The output CSV file will contain the following columns:
- `Backends`: Name of the OpenEO backend
- `URL`: URL of the API endpoint
- `Response Time (ms)`: Response time in milliseconds (if successful)
- `Status`: HTTP status code or error message

## Error Handling

The script handles various error conditions:
- Invalid URLs: Marked as "Invalid URL" in the status column
- Timeouts: Marked as "Timeout" in the status column
- Connection errors: Marked as "Connection Error" in the status column
- Other request exceptions: Error message included in the status column

## Example

```bash
python openeo-checker.py -i api-endpoint.csv -o results.csv
```

## License

MIT License

