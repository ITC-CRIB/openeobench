# openEObench Utilities

This directory contains utility scripts and documentation for the openEObenchmarking project.

## Utility Scripts

### Process Analysis
- **`fetch_process_tests.py`** - Fetches process test definitions from the OpenEO processes repository
- **`count_processes_by_level.py`** - Counts processes by level (L1, L2, L3, L4) from CSV data
- **`generate_process_levels.py`** - Generates process level assignments and mappings

### Data Processing
- **`calculate_statistics.py`** - Calculates compliance statistics across backends
- **`analyze_timing_statistics.py`** - Analyzes timing and performance statistics
- **`convert_to_summary.py`** - Converts detailed results to summary format

## Documentation

### Integration Summaries
- **`CSV_INTEGRATION_SUMMARY.md`** - Summary of CSV integration changes and improvements
- **`comparison_report.md`** - Process comparison analysis report
- **`fetch_process_tests_README.md`** - Documentation for process test fetching

## Usage

Most utilities can be run standalone:

```bash
# Count processes by level
python utils/count_processes_by_level.py

# Fetch latest process tests
python utils/fetch_process_tests.py

# Calculate statistics
python utils/calculate_statistics.py
```

Some utilities may require specific input files or parameters. Check the individual script headers for usage details.

## Dependencies

Utilities may have different dependencies than the main project. Common requirements:
- `requests` - for API calls
- `csv` - for CSV processing  
- `json` - for JSON handling
- `argparse` - for command-line interfaces
