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
python openeobench service --url <apu_url> --output <out> --format <format>
```
Example:

```bash
# Check single endpoint
python openeobench service -u https://openeo.dataspace.copernicus.eu/.well-known/openeo -o results/
```

### Service Summary

Extended service summary with format options:

```bash
python openeobench service-summary <input> --output <out> --format <format>
```
Example:

```bash
# CSV output
python openeobench service-summary -i results/ -o summary.csv

# Markdown report
python openeobench service-summary -i results/ -o summary.md
```

**Output**: 
- CSV columns: `url`, `availability`, `avg_response_time`, `stddev_response_time`, `latency`, `latency_stddev`
- Markdown document with formatted statistics

### Scenario Execution

Run OpenEO workflows on backends:
```bash
openeobench run --url <api_url> --input <graph.json> --output <out>
```

Example:
```bash
python openeobench run --url https://openeo.dataspace.copernicus.eu -i scenario.json -o results/
```

**Output**: Folder containing process graph (JSON), job metadata (JSON), and output files (GeoTIFF, etc.)


### Run Summaries

Generate timing statistics from workflow executions:

```bash
openeobench run-summary <input,...> --output <out> --format <format>
```

Example:
```bash
python openeobench run-summary -i output/folder1 output/folder2 -o timing_summary.csv
```

**Output columns:** `filename`, `time_submit`, `time_submit_stddev`, `time_job_execution`, `time_job_execution_stddev`, `time_download`, `time_download_stddev`, `time_processing`, `time_processing_stddev`, `time_queue`, `time_queue_stddev`, `time_total`, `time_total_stddev`

### Process Compliance Checking

Check OpenEO process availability and compliance:

```bash
# Check single backend
python openeobench process --backend "VITO" --api-url https://openeo.vito.be/openeo/1.1 -o process_results

# Check multiple backends from CSV
python openeobench process -i backends.csv -o process_compliance
```

**Output**: 
- CSV file (`.csv`) with columns: `process`, `level`, `status`, `compatibility`, `reason`
- JSON file (`.json`) containing the raw `/processes` endpoint response
- Process compliance analysis against OpenEO profiles (L1-L4)

### Process Summary

Generate compliance reports for process implementations:

```bash
# CSV output
python openeobench process-summary -i process_results/ -o process_summary.csv

# Markdown report
python openeobench process-summary -i process_results/ -o process_summary.md
```

**Output**: 
- CSV columns: `backend`, `l1_available`, `l1_compliance_rate`, `l2_available`, `l2_compliance_rate`, etc.
- Markdown document with compliance analysis across backends



## Commands Reference

### Current Commands

| Command | Description | Key Options | Dependencies |
|---------|-------------|-------------|--------------|
| `service` | Check endpoint availability | `-i` (CSV file), `-u` (single URL), `-o` (output dir) |
| `service-summary` | Performance reports | `-i` (results folder/CSV), `-o` (CSV/MD output) |
| `run` | Execute OpenEO scenarios | `--api-url` (backend), `-i` (scenario JSON), `-o` (output dir) |
| `run-summary` | Timing statistics from runs | `-i` (result folders/files), `-o` (CSV output) |
| `process` | Check process availability/compliance | `--backend` + `--api-url` or `-i` (CSV), `-o` (output file) | requests |
| `process-summary` | Generate compliance reports | `-i` (results folder/file), `-o` (CSV/MD output) | - |

## OpenEO Process Profiles

The process compliance checking is based on the official OpenEO API specification process profiles:

- **L1 (Basic)**: Essential processes for basic data access and output
  - `load_collection`, `save_result`, `filter_bbox`, `filter_temporal`, `reduce_dimension`, `apply`, `linear_scale_range`
- **L2 (EO Data Manipulation)**: Earth observation specific data processing
  - `ndvi`, `evi`, `aggregate_temporal`, `resample_spatial`, `merge_cubes`, `apply_dimension`, `array_element`, `clip`, `mask`, `filter_bands`
- **L3 (Mathematical Operations)**: Mathematical and statistical functions
  - `add`, `subtract`, `multiply`, `divide`, `absolute`, `sqrt`, `power`, `exp`, `ln`, `log`, `sin`, `cos`, `tan`, `arcsin`, `arccos`, `arctan`, `min`, `max`, `mean`, `median`, `sum`, `product`, `count`, `sd`, `variance`
- **L4 (Advanced Analysis)**: Advanced algorithms and machine learning
  - `fit_curve`, `predict_curve`, `ml_fit`, `ml_predict`, `sar_backscatter`, `atmospheric_correction`, `cloud_detection`, `create_data_cube`

## Output Formats

### Service Check Results
- **Location**: `outputs/YYYY-MM-DD.csv` or `outputs/YYYY-MM-DD_single.csv`
- **Columns**: url, timestamp, response_time, status_code, error_msg, content_size

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
