# OpenEO Bench

**⚠️ This project is under active development.**

A comprehensive benchmarking and testing tool for OpenEO backends and services.

## Overview

OpenEO Bench provides four main functionalities:

- **Service Checking**: Test endpoint availability and response times
- **Scenario Execution**: Run OpenEO workflows on backends  
- **Run Summaries**: Analyze timing statistics from workflow executions
- **Service Summaries**: Generate performance reports from endpoint checks

### Coming Soon

- **Process Analysis**: Check OpenEO process availability and compliance across backends
- **Process Summaries**: Generate compliance reports for process implementations

## Installation

### Using uv (recommended)

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and setup
git clone <repository-url>
cd openeobench

# Install all dependencies (includes GDAL, OpenEO, etc.)
uv sync

# Run commands
uv run python openeobench --help
```

### Using pip

#### Option 1: Install all dependencies (full functionality)

```bash
git clone <repository-url>
cd openeobench

# Install from pyproject.toml (includes all dependencies)
pip install -e .

# Or install specific requirements
pip install -r requirements.txt

python openeobench --help
```

## Usage

### Service Checking

Test OpenEO endpoint availability and response times:

```bash
# Check endpoints from CSV file
python openeobench service -i endpoints.csv -o results/

# Check single endpoint
python openeobench service -u https://openeo.dataspace.copernicus.eu/.well-known/openeo -o results/
```

**Input CSV format:**
```csv
URL
https://openeo.dataspace.copernicus.eu/.well-known/openeo
https://openeo.eodc.eu/.well-known/openeo
```

### Scenario Execution

Run OpenEO workflows on backends:

```bash
python openeobench run --api-url https://openeo.dataspace.copernicus.eu -i scenario.json -o results/
```

### Run Summaries

Generate timing statistics from workflow executions:

```bash
python openeobench run-summary -i output/folder1 output/folder2 -o timing_summary.csv
```

**Output columns:** `filename`, `time_submit`, `time_submit_stddev`, `time_job_execution`, `time_job_execution_stddev`, `time_download`, `time_download_stddev`, `time_processing`, `time_processing_stddev`, `time_queue`, `time_queue_stddev`, `time_total`, `time_total_stddev`

### Service Summaries

Generate performance reports from endpoint checks:

```bash
# CSV output
python openeobench service-summary -i results/ -o summary.csv

# Markdown report
python openeobench service-summary -i results/ -o summary.md
```

## Coming Soon

### Process Analysis

Check OpenEO process availability and compliance:

```bash
python openeobench process --url <api_url> --output <out>
```

**Output**: 
- CSV file with columns: `process`, `level`, `status`, `compatibility`, `reason`
- OpenEO API compliant JSON file with detailed process information

### Enhanced Run Command

Simplified execution with URL parameter:

```bash
python openeobench run --url <api_url> --input <graph.json> --output <out>
```

**Output**: Folder containing process graph (JSON), job metadata (JSON), and output files (GeoTIFF, etc.)

### Enhanced Service Summary

Extended service summary with format options:

```bash
python openeobench service-summary <input> --output <out> --format <format>
```

**Output**: 
- CSV columns: `url`, `availability`, `avg_response_time`, `stddev_response_time`, `latency`, `latency_stddev`
- Markdown document with formatted statistics

### Process Summary

Generate compliance reports for process implementations:

```bash
python openeobench process-summary <input,...> --output <out> --format <format>
```

**Output**: 
- CSV columns: `backend`, `l1_available`, `l1_missing`, `l1_mismatch`, `l2_available`, `...`, `total_mismatch`
- Markdown document with compliance analysis across backends

## Commands Reference

### Current Commands

| Command | Description | Key Options | Dependencies |
|---------|-------------|-------------|--------------|
| `service` | Check endpoint availability | `-i` (CSV file), `-u` (single URL), `-o` (output dir) | `requests` |
| `run` | Execute OpenEO scenarios | `--api-url` (backend), `-i` (scenario JSON), `-o` (output dir) | `openeo`, `gdal`, etc. |
| `run-summary` | Timing statistics from runs | `-i` (result folders/files), `-o` (CSV output) | Basic Python |
| `service-summary` | Performance reports | `-i` (results folder/CSV), `-o` (CSV/MD output) | Basic Python |

### Coming Soon Commands

| Command | Description | Key Options | Output Format |
|---------|-------------|-------------|---------------|
| `process` | Check process availability/compliance | `--url` (API URL), `--output` (directory) | CSV + JSON |
| `process-summary` | Generate compliance reports | `<input,...>` (inputs), `--output`, `--format` | CSV/Markdown |
| Enhanced `service-summary` | Extended statistics | `<input>`, `--output`, `--format` | CSV/Markdown |
| Enhanced `run` | Simplified execution | `--url`, `--input`, `--output` | Folder structure |

## Output Formats

### Service Check Results
- **Location**: `outputs/YYYY-MM-DD.csv` or `outputs/YYYY-MM-DD_single.csv`
- **Columns**: URL, Timestamp, Response Time (ms), HTTP Code, Errors, Body Size (bytes)

### Run Results  
- **Location**: Organized in timestamped folders with `results.json`
- **Contains**: Timing data, job status, process graphs, downloaded results

### Summary Reports
- **CSV**: Machine-readable statistics and timing data
- **Markdown**: Human-readable formatted reports with tables

## File Structure

```
openeobench/
├── openeobench           # Main CLI script
├── openeo_checker.py     # Core functionality
├── openeotest.py         # Scenario execution engine
├── scenarios/            # Test scenario definitions
├── outputs/              # Service check results
└── README.md
```

## Development Status

Current areas of development:
- Enhanced authentication flow testing


## Requirements

### System Requirements
- Python 3.13+ (as specified in pyproject.toml)
- GDAL system libraries (for geospatial functionality)

### Python Dependencies

**Core dependencies** (from pyproject.toml):
- `requests>=2.32.4` - HTTP client for API calls
- `openeo>=0.42.1` - OpenEO Python client
- `gdal[numpy]==3.8.4` - Geospatial data processing
- `numpy>=2.3.0` - Numerical computing
- `matplotlib>=3.10.3` - Plotting and visualization
- `rioxarray>=0.19.0` - Raster data handling

### Installation Notes

- **GDAL requirement**: May require system-level GDAL installation on some platforms
- All commands require the full dependency set for proper functionality

## License

MIT License
