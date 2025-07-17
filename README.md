# OpenEO Bench

**⚠️ This project is under active development.**

A comprehensive benchmarking and testing tool for OpenEO backends and services.

## Overview

OpenEO Bench provides comprehensive functionality for testing and benchmarking OpenEO backends:

- **Service Checking**: Test endpoint availability and response times
- **Scenario Execution**: Run OpenEO workflows on backends  
- **Run Summaries**: Analyze timing statistics from workflow executions
- **Result Summaries**: Generate comprehensive statistics from workflow outputs
- **Service Summaries**: Generate performance reports from endpoint checks
- **Process Analysis**: Check OpenEO process availability and compliance across backends
- **Process Summaries**: Generate compliance reports for process implementations
- **Visualization**: Create visual matrices and reports of GeoTIFF results

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
# Check endpoints from CSV file (creates new timestamped file)
openeobench service -i endpoints.csv -o results/

# Check endpoints and append to daily file
openeobench service -i endpoints.csv -o results/ --append

# Check a single URL (creates new timestamped file)
openeobench service -u https://openeo.dataspace.copernicus.eu/.well-known/openeo -o results/

# Check a single URL and append to daily file
openeobench service -u https://openeo.dataspace.copernicus.eu/.well-known/openeo -o results/ --append
```

### Service Summary

Generate performance reports from endpoint check results:

```bash
# Generate statistics summary from folder (CSV output)
openeobench service-summary -i results/ -o summary.csv

# Generate statistics summary from folder (Markdown output)
openeobench service-summary -i results/ -o summary.md

# Generate statistics from single CSV file
openeobench service-summary -i results/2025-06-26.csv -o summary.md
```

**Output**: 
- CSV columns: `url`, `availability`, `avg_response_time`, `stddev_response_time`, `latency`, `latency_stddev`
- Markdown document with formatted statistics

### Scenario Execution

Run OpenEO workflows on backends:

```bash
# Run OpenEO scenario on a backend
openeobench run --api-url https://openeo.dataspace.copernicus.eu -i scenario.json -o results/
```

**Output**: Folder containing process graph (JSON), job metadata (JSON), and output files (GeoTIFF, etc.)


### Run Summaries

Generate timing statistics from workflow executions:

```bash
# Generate timing summary from run results
openeobench run-summary -i output/folder1 output/folder2 -o timing_summary.csv
```

**Output columns:** `filename`, `time_submit`, `time_submit_stddev`, `time_job_execution`, `time_job_execution_stddev`, `time_download`, `time_download_stddev`, `time_processing`, `time_processing_stddev`, `time_queue`, `time_queue_stddev`, `time_total`, `time_total_stddev`

### Result Summaries

Generate comprehensive statistics from workflow outputs:

```bash
# Generate file statistics summary from run results (CSV)
openeobench result-summary output/folder1 output/folder2 --output file_stats.csv

# Generate file statistics summary from run results (Markdown)
openeobench result-summary output/folder1 output/folder2 --output file_stats.md --format md
```

**Output**: Comprehensive statistics about generated files, data types, sizes, and processing results

### Process Compliance Checking

Check OpenEO process availability and compliance:

```bash
# Check process compliance for single backend
openeobench process --url https://openeo.vito.be/openeo/1.1 -o process_results.csv

# Check process compliance for multiple backends
openeobench process -i backends.csv -o process_compliance.csv
```

**Output**: 
- CSV file (`.csv`) with columns: `process`, `level`, `status`, `compatibility`, `reason`
- JSON file (`.json`) containing the raw `/processes` endpoint response
- Process compliance analysis against OpenEO profiles (L1-L4)

### Process Summary

Generate compliance reports for process implementations:

```bash
# Generate process compliance summary (CSV)
openeobench process-summary process_results/ --output process_summary.csv --format csv

# Generate process compliance summary (Markdown)  
openeobench process-summary process_results/ --output process_summary.md --format md
```

**Output**: 
- CSV columns: `backend`, `l1_available`, `l1_compliance_rate`, `l2_available`, `l2_compliance_rate`, etc.
- Markdown document with compliance analysis across backends

### Visualization

Create visual matrices and reports of GeoTIFF results:

```bash
# Create visual matrix of GeoTIFF results with format options
openeobench visualize output/folder1 output/folder2 --output visualization.md --format both

# Create only PNG matrix
openeobench visualize output/folder1 output/folder2 --output visualization.png --format png

# Create only markdown report
openeobench visualize output/folder1 output/folder2 --output visualization.md --format md

# Visualize individual TIFF files
openeobench visualize path/to/file1.tif path/to/file2.tif --output comparison.md --format both

# Mix folders and individual files
openeobench visualize output/folder1 path/to/specific_file.tif --output mixed_visualization.md
```

**Output**: 
- Markdown reports with embedded image matrices and statistics
- PNG matrix visualizations showing all results in a single image
- Individual PNG files for each GeoTIFF
- Support for folders, individual files, or mixed inputs
- **Note**: CSV statistics output has been removed as of recent updates



## Commands Reference

### Current Commands

| Command | Description | Key Options | Dependencies |
|---------|-------------|-------------|--------------|
| `service` | Check endpoint availability | `-i` (CSV file), `-u` (single URL), `-o` (output dir), `--append` |
| `service-summary` | Performance reports | `-i` (results folder/CSV), `-o` (CSV/MD output) |
| `run` | Execute OpenEO scenarios | `--api-url` (backend), `-i` (scenario JSON), `-o` (output dir) |
| `run-summary` | Timing statistics from runs | `-i` (result folders/files), `-o` (CSV output) |
| `result-summary` | Comprehensive file statistics | Input folders/files, `--output` (CSV/MD), `--format` |
| `process` | Check process availability/compliance | `--url` (single backend) or `-i` (CSV), `-o` (output file) | requests |
| `process-summary` | Generate compliance reports | `-i` (results folder/file), `--output` (CSV/MD), `--format` | - |
| `visualize` | Create visual matrices of GeoTIFF results | Input folders/files, `--output` (MD/PNG), `--format` (md/png/both) | GDAL, matplotlib |

### Complete Usage Examples

```bash
# Check endpoints from CSV file (creates new timestamped file)
openeobench service -i endpoints.csv -o results/

# Check endpoints and append to daily file
openeobench service -i endpoints.csv -o results/ --append

# Check a single URL (creates new timestamped file)
openeobench service -u https://openeo.example.com/.well-known/openeo -o results/

# Check a single URL and append to daily file
openeobench service -u https://openeo.example.com/.well-known/openeo -o results/ --append

# Run OpenEO scenario on a backend
openeobench run --api-url https://openeo.example.com -i scenario.json -o results/

# Generate timing summary from run results
openeobench run-summary -i output/folder1 output/folder2 -o timing_summary.csv

# Generate file statistics summary from run results (CSV)
openeobench result-summary output/folder1 output/folder2 --output file_stats.csv

# Generate file statistics summary from run results (Markdown)
openeobench result-summary output/folder1 output/folder2 --output file_stats.md --format md

# Generate statistics summary from folder (CSV output)
openeobench service-summary -i results/ -o summary.csv

# Generate statistics summary from folder (Markdown output)
openeobench service-summary -i results/ -o summary.md

# Generate statistics from single CSV file
openeobench service-summary -i results/2025-06-26.csv -o summary.md

# Check process compliance for single backend
openeobench process --url https://openeo.vito.be/openeo/1.1 -o process_results.csv

# Check process compliance for multiple backends
openeobench process -i backends.csv -o process_compliance.csv

# Generate process compliance summary (CSV)
openeobench process-summary process_results/ --output process_summary.csv --format csv

# Generate process compliance summary (Markdown)  
openeobench process-summary process_results/ --output process_summary.md --format md

# Create visual matrix of GeoTIFF results with format options
openeobench visualize output/folder1 output/folder2 --output visualization.md --format both

# Create only PNG matrix
openeobench visualize output/folder1 output/folder2 --output visualization.png --format png

# Create only markdown report
openeobench visualize output/folder1 output/folder2 --output visualization.md --format md

# Visualize individual TIFF files
openeobench visualize path/to/file1.tif path/to/file2.tif --output comparison.md --format both

# Mix folders and individual files
openeobench visualize output/folder1 path/to/specific_file.tif --output mixed_visualization.md
```

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
