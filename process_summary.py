#!/usr/bin/env python3
"""
Cross-backend process compliance summary generator for OpenEO.
Analyzes process compliance across multiple backends and generates summary reports.
"""

import csv
import json
import sys
import argparse
import os
from typing import Dict, List, Any

def load_process_results(input_path: str) -> List[Dict]:
    """
    Load process check results from file or directory.
    
    Args:
        input_path: Path to CSV file or directory containing CSV files
        
    Returns:
        List of result dictionaries
    """
    results = []
    
    if os.path.isfile(input_path):
        # Single file
        if input_path.endswith('.csv'):
            results.extend(load_csv_file(input_path))
        elif input_path.endswith('.json'):
            results.extend(load_json_file(input_path))
    elif os.path.isdir(input_path):
        # Directory - load all CSV files
        for filename in os.listdir(input_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(input_path, filename)
                results.extend(load_csv_file(file_path))
            elif filename.endswith('.json'):
                file_path = os.path.join(input_path, filename)
                results.extend(load_json_file(file_path))
    
    return results

def load_csv_file(file_path: str) -> List[Dict]:
    """Load results from CSV file."""
    results = []
    try:
        # Detect CSV format
        csv_format = detect_csv_format(file_path)
        
        if csv_format == 'process_level':
            # Handle new process-level format
            summary = aggregate_process_level_data(file_path)
            if summary:
                # Add source file information for mismatch counting
                summary['source_file'] = file_path
                results.append(summary)
        elif csv_format == 'backend_summary':
            # Handle legacy backend summary format
            with open(file_path, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Convert string values to appropriate types
                    result = {}
                    for key, value in row.items():
                        if key in ['success']:
                            result[key] = value.lower() in ('true', '1', 'yes')
                        elif key in ['total_processes', 'response_time', 'status_code']:
                            try:
                                result[key] = float(value) if '.' in value else int(value)
                            except (ValueError, TypeError):
                                result[key] = value
                        elif key.endswith('_available') or key.endswith('_total') or key == 'custom':
                            try:
                                result[key] = int(value) if value else 0
                            except (ValueError, TypeError):
                                result[key] = 0
                        elif key.endswith('_compliance_rate'):
                            try:
                                result[key] = float(value) if value else 0.0
                            except (ValueError, TypeError):
                                result[key] = 0.0
                        else:
                            result[key] = value
                    results.append(result)
        else:
            print(f"Warning: Unknown CSV format in {file_path}")
            
    except Exception as e:
        print(f"Error loading CSV file {file_path}: {e}")
    
    return results

def load_json_file(file_path: str) -> List[Dict]:
    """Load results from JSON file."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            else:
                return [data]
    except Exception as e:
        print(f"Error loading JSON file {file_path}: {e}")
        return []

def generate_process_summary(results: List[Dict]) -> Dict[str, Any]:
    """
    Generate comprehensive process compliance summary.
    
    Args:
        results: List of process check results
        
    Returns:
        Summary dictionary with statistics and analysis
    """
    summary = {
        'total_backends': len(results),
        'successful_checks': sum(1 for r in results if r.get('success', False)),
        'failed_checks': sum(1 for r in results if not r.get('success', False)),
        'backends': [],
        'profile_summary': {},
        'overall_statistics': {}
    }
    
    # Process each backend
    successful_results = [r for r in results if r.get('success', False)]
    
    for result in results:
        backend_info = {
            'backend': result.get('backend', 'unknown'),
            'api_url': result.get('api_url', ''),
            'success': result.get('success', False),
            'total_processes': result.get('total_processes', 0),
            'response_time': result.get('response_time', 0),
            'error': result.get('error', '') if not result.get('success', False) else ''
        }
        
        # Add compliance data if successful
        if result.get('success', False):
            for profile in ['l1', 'l2', 'l3', 'l4', 'overall']:
                available = result.get(f'{profile}_available', 0)
                total = result.get(f'{profile}_total', 0)
                rate = result.get(f'{profile}_compliance_rate', 0.0)
                mismatch = result.get(f'{profile}_mismatch', 0)  # Preserve mismatch data
                experimental = result.get(f'{profile}_experimental', 0)  # Add experimental data
                
                backend_info[f'{profile}_compliance'] = {
                    'available': available,
                    'total': total,
                    'rate': rate,
                    'experimental': experimental,
                    'percentage': f"{rate * 100:.1f}%"
                }
                
                # Store mismatch data separately
                backend_info[f'{profile}_mismatch'] = mismatch
            
            # Handle custom processes separately
            custom_count = result.get('custom', 0)
            custom_total = result.get('custom_total', 0)
            custom_rate = result.get('custom_compliance_rate', 0.0)
            
            backend_info['custom_compliance'] = {
                'available': custom_count,
                'total': custom_total,
                'rate': custom_rate,
                'percentage': f"{custom_rate * 100:.1f}%"
            }
        
        summary['backends'].append(backend_info)
    
    # Calculate profile summaries
    profiles = ['l1', 'l2', 'l3', 'l4', 'custom', 'overall']
    for profile in profiles:
        profile_data = {
            'total_backends_checked': len(successful_results),
            'avg_compliance_rate': 0.0,
            'min_compliance_rate': 1.0,
            'max_compliance_rate': 0.0,
            'full_compliance_count': 0,
            'backend_compliance': []
        }
        
        compliance_rates = []
        for result in successful_results:
            rate = result.get(f'{profile}_compliance_rate', 0.0)
            compliance_rates.append(rate)
            
            profile_data['backend_compliance'].append({
                'backend': result.get('backend', 'unknown'),
                'available': result.get(f'{profile}_available', 0),
                'total': result.get(f'{profile}_total', 0),
                'rate': rate,
                'percentage': f"{rate * 100:.1f}%"
            })
        
        if compliance_rates:
            profile_data['avg_compliance_rate'] = sum(compliance_rates) / len(compliance_rates)
            profile_data['min_compliance_rate'] = min(compliance_rates)
            profile_data['max_compliance_rate'] = max(compliance_rates)
            profile_data['full_compliance_count'] = sum(1 for rate in compliance_rates if rate >= 1.0)
        
        # Add percentages
        profile_data['avg_compliance_percentage'] = f"{profile_data['avg_compliance_rate'] * 100:.1f}%"
        profile_data['min_compliance_percentage'] = f"{profile_data['min_compliance_rate'] * 100:.1f}%"
        profile_data['max_compliance_percentage'] = f"{profile_data['max_compliance_rate'] * 100:.1f}%"
        
        summary['profile_summary'][profile] = profile_data
    
    # Overall statistics
    if successful_results:
        total_processes = [r.get('total_processes', 0) for r in successful_results]
        response_times = [r.get('response_time', 0) for r in successful_results]
        
        summary['overall_statistics'] = {
            'avg_total_processes': sum(total_processes) / len(total_processes),
            'min_total_processes': min(total_processes),
            'max_total_processes': max(total_processes),
            'avg_response_time': sum(response_times) / len(response_times),
            'min_response_time': min(response_times),
            'max_response_time': max(response_times)
        }
    
    return summary

def write_csv_summary(summary: Dict[str, Any], output_file: str):
    """Write summary to CSV format with backend compliance columns."""
    
    fieldnames = [
        'backend', 'l1_available', 'l1_missing', 'l1_mismatch',
        'l2_available', 'l2_missing', 'l2_mismatch',
        'l3_available', 'l3_missing', 'l3_mismatch', 
        'l4_available', 'l4_missing', 'l4_mismatch',
        'custom',
        'total_available', 'total_missing', 'total_mismatch'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for backend in summary['backends']:
            if not backend['success']:
                continue  # Skip failed backends
                
            row = {'backend': backend.get('backend', backend.get('api_url', ''))}
            
            # Get mismatch counts from source file if available
            source_file = backend.get('source_file')
            mismatch_counts = {}
            if source_file:
                mismatch_counts = count_mismatches_from_csv(source_file)
            
            # Add compliance data for each profile
            for profile in ['l1', 'l2', 'l3', 'l4']:
                # Check for new process-level aggregated format first
                if f'{profile}_available' in backend:
                    # New format: direct values from aggregate_process_level_data
                    available = backend.get(f'{profile}_available', 0)
                    total = backend.get(f'{profile}_total', 0)
                    missing = total - available
                    # Use stored mismatch or calculate from source file
                    mismatch = backend.get(f'{profile}_mismatch', mismatch_counts.get(profile, 0))
                    
                    row[f'{profile}_available'] = available
                    row[f'{profile}_missing'] = missing
                    row[f'{profile}_mismatch'] = mismatch
                elif f'{profile}_compliance' in backend:
                    # Legacy format: compliance structure
                    comp = backend[f'{profile}_compliance']
                    available = comp['available']
                    total_processes = comp['total']
                    missing = total_processes - available
                    
                    # For legacy format, use calculated mismatches from source file
                    mismatch = backend.get(f'{profile}_mismatch', mismatch_counts.get(profile, 0))
                    
                    row[f'{profile}_available'] = available
                    row[f'{profile}_missing'] = missing
                    row[f'{profile}_mismatch'] = mismatch
                else:
                    row[f'{profile}_available'] = 0
                    row[f'{profile}_missing'] = 0
                    row[f'{profile}_mismatch'] = 0
            
            # Handle custom processes separately (just count, no missing/mismatch)
            if 'custom_compliance' in backend:
                custom_comp = backend['custom_compliance']
                row['custom'] = custom_comp['available']
            else:
                row['custom'] = 0
            
            # Use overall compliance for totals (unique processes across all profiles)
            if 'overall_available' in backend:
                # New format: direct values from aggregate_process_level_data
                row['total_available'] = backend.get('overall_available', 0)
                row['total_missing'] = backend.get('overall_total', 0) - backend.get('overall_available', 0)
                # Use stored mismatch or calculate from source file
                row['total_mismatch'] = backend.get('overall_mismatch', mismatch_counts.get('overall', 0))
            elif 'overall_compliance' in backend:
                # Legacy format: compliance structure
                overall_comp = backend['overall_compliance']
                row['total_available'] = overall_comp['available']
                row['total_missing'] = overall_comp['total'] - overall_comp['available']
                # Use calculated mismatches from source file
                row['total_mismatch'] = backend.get('overall_mismatch', mismatch_counts.get('overall', 0))
            else:
                row['total_available'] = 0
                row['total_missing'] = 0
                row['total_mismatch'] = 0
            
            writer.writerow(row)

def write_markdown_summary(summary: Dict[str, Any], output_file: str):
    """Write summary to Markdown format with custom table layout."""
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# OpenEO Process Compliance Summary\n\n")
        
        # Main compliance table with the requested format
        f.write("| Platform | Version | L1 - Minimal | L2 - Advanced | L3 - Advanced | L4 - A&B | Custom |\n")
        f.write("|----------|---------|-------------|--------------|--------------|----------|--------|\n")
        
        for backend in summary['backends']:
            if not backend['success']:
                continue  # Skip failed backends
                
            # Extract platform name from URL or backend name
            backend_url = backend.get('api_url', '')
            backend_name = backend.get('backend', '')
            
            if backend_url:
                platform_name = extract_platform_name(backend_url)
            elif backend_name:
                platform_name = backend_name
            else:
                platform_name = 'Unknown'
            
            # Extract version - this would need to be added to the backend data
            # For now, we'll use a placeholder or try to extract from URL
            version = extract_version_from_url(backend_url)
            
            # Format each profile column as: available (experimental, missing)
            l1_data = format_profile_data(backend.get('l1_compliance', {}))
            l2_data = format_profile_data(backend.get('l2_compliance', {}))
            l3_data = format_profile_data(backend.get('l3_compliance', {}))
            l4_data = format_profile_data(backend.get('l4_compliance', {}))
            
            # Custom processes count
            custom_count = backend.get('custom_compliance', {}).get('available', 0)
            
            f.write(f"| {platform_name} | {version} | {l1_data} | {l2_data} | {l3_data} | {l4_data} | {custom_count} |\n")
        
        f.write("\n*Format: [Available (Experimental_Available, Missing)]*\n\n")
        
        # Optional: Add overview section
        f.write("## Overview\n\n")
        f.write(f"- **Total Backends Analyzed**: {summary['total_backends']}\n")
        f.write(f"- **Successful Checks**: {summary['successful_checks']}\n")
        f.write(f"- **Failed Checks**: {summary['failed_checks']}\n\n")
        
        # Profile Definitions
        f.write("## Profile Definitions\n\n")
        f.write("- **L1 - Minimal**: Basic processes (load_collection, save_result, filter_bbox, etc.)\n")
        f.write("- **L2 - Advanced**: EO data manipulation (ndvi, evi, aggregate_temporal, etc.)\n") 
        f.write("- **L3 - Advanced**: Mathematical operations (add, subtract, sin, cos, etc.)\n")
        f.write("- **L4 - A&B**: Advanced analysis (fit_curve, ml_fit, sar_backscatter, etc.)\n")
        f.write("- **Custom**: Backend-specific processes not in standard profiles\n\n")

def detect_csv_format(file_path: str) -> str:
    """
    Detect the CSV format - either 'backend_summary' or 'process_level'.
    
    Args:
        file_path: Path to CSV file
        
    Returns:
        Format type: 'backend_summary' or 'process_level'
    """
    try:
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader, [])
            
            # Check for process-level format
            if 'process' in header and 'level' in header and 'status' in header:
                return 'process_level'
            # Check for backend summary format  
            elif 'backend' in header and 'total_processes' in header:
                return 'backend_summary'
            else:
                return 'unknown'
    except Exception:
        return 'unknown'

def load_process_levels_data():
    """Load process levels data with experimental flags."""
    process_levels = {}
    try:
        # Look for process levels file in common locations
        possible_paths = [
            'openeo-process-levels.csv',
            os.path.join(os.path.dirname(__file__), 'openeo-process-levels.csv'),
            os.path.join(os.getcwd(), 'openeo-process-levels.csv')
        ]
        
        levels_file = None
        for path in possible_paths:
            if os.path.exists(path):
                levels_file = path
                break
        
        if not levels_file:
            return {}
        
        with open(levels_file, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                process_name = row['process']
                level = row['level']
                is_experimental = row['experimental'].lower() == 'yes'
                process_levels[process_name] = {
                    'level': level,
                    'experimental': is_experimental
                }
    except Exception as e:
        print(f"Warning: Could not load process levels data: {e}")
    
    return process_levels

def aggregate_process_level_data(file_path: str) -> Dict:
    """
    Aggregate process-level CSV data into backend summary.
    
    Args:
        file_path: Path to process-level CSV file
        
    Returns:
        Backend summary dictionary
    """
    try:
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            processes = list(reader)
        
        if not processes:
            return {}
        
        # Load process levels data to get experimental flags
        process_levels = load_process_levels_data()
        
        # Extract backend name from filename or use default
        backend_name = os.path.splitext(os.path.basename(file_path))[0]
        if backend_name.startswith('demo_'):
            backend_name = backend_name.replace('demo_', '').replace('_real', '').upper()
        elif 'cdse' in backend_name.lower():
            backend_name = 'CDSE'
        elif 'vito' in backend_name.lower():
            backend_name = 'VITO'
        elif 'eodc' in backend_name.lower():
            backend_name = 'EODC'
        elif 'earthengine' in backend_name.lower():
            backend_name = 'Google Earth Engine'
        else:
            backend_name = backend_name.replace('_', ' ').title()
        
        # Count processes by level and status
        level_stats = {}
        total_processes = len(processes)
        custom_processes = []
        
        for process in processes:
            level = process.get('level', '').lower()
            status = process.get('status', '')
            compatibility = process.get('compatibility', '')
            process_name = process.get('process', '')
            
            if level not in level_stats:
                level_stats[level] = {
                    'available': 0, 
                    'total': 0, 
                    'missing': [], 
                    'mismatch': 0,
                    'experimental': 0,
                    'experimental_available': 0
                }
            
            level_stats[level]['total'] += 1
            
            # Check if process is experimental
            is_experimental = process_levels.get(process_name, {}).get('experimental', False)
            if is_experimental:
                level_stats[level]['experimental'] += 1
            
            if status == 'available':
                level_stats[level]['available'] += 1
                if is_experimental:
                    level_stats[level]['experimental_available'] += 1
                # Check for parameter mismatches
                if compatibility == 'mismatch':
                    level_stats[level]['mismatch'] += 1
            else:
                level_stats[level]['missing'].append(process_name)
            
            # Track custom processes
            if level == 'custom' and status == 'available':
                custom_processes.append(process_name)
        
        # Build summary
        summary = {
            'backend': backend_name,
            'api_url': '',  # Not available in process-level data
            'success': True,
            'total_processes': total_processes,
            'response_time': 0,  # Not available in process-level data
            'status_code': 200,  # Assume success
            'custom': len(custom_processes),
            'custom_processes': custom_processes
        }
        
        # Add level-specific statistics
        for level in ['l1', 'l2', 'l3', 'l4']:
            if level in level_stats:
                available = level_stats[level]['available']
                total = level_stats[level]['total']
                mismatch = level_stats[level]['mismatch']
                experimental = level_stats[level]['experimental_available']
                rate = available / total if total > 0 else 0.0
                
                summary[f'{level}_available'] = available
                summary[f'{level}_total'] = total
                summary[f'{level}_mismatch'] = mismatch
                summary[f'{level}_experimental'] = experimental
                summary[f'{level}_compliance_rate'] = rate
                summary[f'{level}_missing'] = level_stats[level]['missing']
            else:
                summary[f'{level}_available'] = 0
                summary[f'{level}_total'] = 0
                summary[f'{level}_mismatch'] = 0
                summary[f'{level}_compliance_rate'] = 0.0
                summary[f'{level}_missing'] = []
        
        # Calculate overall statistics (excluding custom)
        overall_available = sum(level_stats.get(level, {}).get('available', 0) for level in ['l1', 'l2', 'l3', 'l4'])
        overall_total = sum(level_stats.get(level, {}).get('total', 0) for level in ['l1', 'l2', 'l3', 'l4'])
        overall_mismatch = sum(level_stats.get(level, {}).get('mismatch', 0) for level in ['l1', 'l2', 'l3', 'l4'])
        overall_rate = overall_available / overall_total if overall_total > 0 else 0.0
        
        summary['overall_available'] = overall_available
        summary['overall_total'] = overall_total
        summary['overall_mismatch'] = overall_mismatch
        summary['overall_compliance_rate'] = overall_rate
        
        return summary
        
    except Exception as e:
        print(f"Error aggregating process-level data from {file_path}: {e}")
        return {}

def count_mismatches_from_csv(file_path: str) -> Dict[str, int]:
    """
    Count mismatches by level from a process-level CSV file.
    
    Args:
        file_path: Path to the process-level CSV file
        
    Returns:
        Dictionary with mismatch counts per level
    """
    mismatch_counts = {'l1': 0, 'l2': 0, 'l3': 0, 'l4': 0, 'overall': 0}
    
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                level = row.get('level', '').lower()
                compatibility = row.get('compatibility', '')
                status = row.get('status', '')
                
                if status == 'available' and compatibility == 'mismatch':
                    if level in mismatch_counts:
                        mismatch_counts[level] += 1
                    mismatch_counts['overall'] += 1
                    
    except Exception:
        pass  # Return zero counts on error
    
    return mismatch_counts

def extract_platform_name(url: str) -> str:
    """Extract platform name from backend URL."""
    if 'dataspace.copernicus' in url or 'cdse' in url:
        return 'CDSE'
    elif 'eo4eu' in url:
        return 'EO4EU'
    elif 'eodc' in url:
        return 'EODC'
    elif 'eurac' in url:
        return 'EURAC'
    elif 'vito' in url and 'openeocloud' not in url:
        return 'VITO'
    elif 'openeocloud' in url or 'openeo-platform' in url:
        return 'PLATFORM'
    elif 'earthengine' in url:
        return 'GEE'
    else:
        # Extract domain name as fallback
        from urllib.parse import urlparse
        try:
            parsed = urlparse(url)
            domain_parts = parsed.netloc.split('.')
            if len(domain_parts) >= 2:
                return domain_parts[-2].upper()
            return parsed.netloc.upper()
        except Exception:
            return 'UNKNOWN'

def extract_version_from_url(url: str) -> str:
    """Extract version from backend URL."""
    # Common version patterns in OpenEO URLs
    if '1.2' in url:
        return '1.2'
    elif '1.1' in url:
        return '1.1'
    elif '1.0' in url:
        return '1.0'
    elif 'v1.0' in url:
        return '1.0'
    else:
        return '1.2'  # Default assumption for most backends

def format_profile_data(compliance_data: Dict) -> str:
    """Format profile compliance data as: available (experimental, missing)."""
    if not compliance_data:
        return "0 (0, 0)"
    
    available = compliance_data.get('available', 0)
    total = compliance_data.get('total', 0)
    missing = total - available
    
    # Try to get experimental count from detailed compliance data
    experimental = compliance_data.get('experimental', 0)
    
    return f"{available} ({experimental}, {missing})"

def main():
    parser = argparse.ArgumentParser(
        description='Generate cross-backend process compliance summary',
        epilog="""
Examples:
  # Generate summary from results directory  
  python process_summary.py input1 input2 --output process_summary.csv --format csv
  
  # Generate Markdown summary
  python process_summary.py results/ --output process_summary.md --format md
  
  # Analyze single results file
  python process_summary.py process_results.csv --output summary.csv
        """
    )
    
    parser.add_argument('input', nargs='+', 
                       help='Input directories or files with process check results')
    parser.add_argument('--output', required=True,
                       help='Output file (.csv or .md)')
    parser.add_argument('--format', choices=['csv', 'md'], 
                       help='Output format (csv or md). If not specified, inferred from output file extension')
    
    args = parser.parse_args()
    
    # Validate input paths
    for input_path in args.input:
        if not os.path.exists(input_path):
            print(f"Error: Input path '{input_path}' does not exist")
            return 1
    
    # Load results from all inputs
    results = []
    for input_path in args.input:
        print(f"Loading process results from: {input_path}")
        input_results = load_process_results(input_path)
        results.extend(input_results)
    
    if not results:
        print("No process results found")
        return 1
    
    print(f"Loaded {len(results)} process check results")
    
    # Generate summary
    summary = generate_process_summary(results)
    
    # Determine output format
    output_format = args.format
    if not output_format:
        output_format = 'md' if args.output.endswith('.md') else 'csv'
    
    # Write output
    if output_format == 'md':
        write_markdown_summary(summary, args.output)
    else:
        write_csv_summary(summary, args.output)
    
    print(f"Summary saved to: {args.output}")
    
    # Print brief console summary
    print(f"\nSummary: {summary['successful_checks']}/{summary['total_backends']} backends checked successfully")
    if 'overall' in summary['profile_summary']:
        overall = summary['profile_summary']['overall']
        print(f"Average overall compliance: {overall['avg_compliance_percentage']}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
