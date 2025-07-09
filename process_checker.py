#!/usr/bin/env python3
"""
Process availability and compliance checker for OpenEO backends.
Checks process compliance against OpenEO process profiles (L1-L4).
"""

import argparse
import csv
import datetime
import json
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

import requests


def load_process_profiles_from_csv(csv_file: str = "openeo-process-levels.csv") -> Dict[str, Dict]:
    """
    Load OpenEO process profiles from CSV file.
    Groups sub-levels (e.g., l2a, l2b, l3-ml, l3-udf) under their main levels (L2, L3, etc.).
    
    Args:
        csv_file: Path to the CSV file containing process levels
        
    Returns:
        Dictionary mapping level names to dictionaries containing:
        - 'processes': Set of all process names in this level
        - 'experimental': Set of experimental process names in this level
        - 'stable': Set of stable (non-experimental) process names in this level
    """
    profiles = {}
    csv_path = Path(csv_file)
    
    if not csv_path.exists():
        # Try relative to script location
        script_dir = Path(__file__).parent
        csv_path = script_dir / csv_file
        
    if not csv_path.exists():
        raise FileNotFoundError(f"Process levels CSV file not found: {csv_file}")
    
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            process_name = row['process']
            level = row['level']
            is_duplicate = row['duplicate'].lower() == 'yes'
            is_experimental = row['experimental'].lower() == 'yes'
            
            # Skip duplicates - only keep the first occurrence
            if is_duplicate:
                continue
                
            # Map sub-levels to main levels
            # l1 -> L1
            # l2, l2a, l2b, l2-date, l2-text -> L2  
            # l3, l3-ml, l3-udf, l3-clim, l3-ard -> L3
            # l4 -> L4
            if level.startswith('l1'):
                level_key = 'L1'
            elif level.startswith('l2'):
                level_key = 'L2'
            elif level.startswith('l3'):
                level_key = 'L3'
            elif level.startswith('l4'):
                level_key = 'L4'
            else:
                # Keep other levels as-is (uppercase)
                level_key = level.upper()
            
            if level_key not in profiles:
                profiles[level_key] = {
                    'processes': set(),
                    'experimental': set(),
                    'stable': set()
                }
            
            profiles[level_key]['processes'].add(process_name)
            if is_experimental:
                profiles[level_key]['experimental'].add(process_name)
            else:
                profiles[level_key]['stable'].add(process_name)
    
    return profiles


def get_legacy_profiles() -> Dict[str, Set[str]]:
    """
    Convert new profile format to legacy format for backward compatibility.
    
    Returns:
        Dictionary mapping level names to sets of process names (all processes)
    """
    detailed_profiles = load_process_profiles_from_csv()
    legacy_profiles = {}
    for level, info in detailed_profiles.items():
        legacy_profiles[level] = info['processes']
    return legacy_profiles


# Load process profiles from CSV
try:
    DETAILED_PROFILES = load_process_profiles_from_csv()
    ALL_PROFILES = get_legacy_profiles()  # For backward compatibility
except FileNotFoundError as e:
    print(f"Error: {e}", file=sys.stderr)
    print("Please ensure openeo-process-levels.csv is in the current directory or script directory.", file=sys.stderr)
    sys.exit(1)

def get_backend_processes(api_url: str) -> Dict:
    """
    Retrieve available processes from an OpenEO backend.
    
    Args:
        api_url: Base URL of the OpenEO backend
        
    Returns:
        Dictionary with process information or error details
    """
    try:
        # Ensure URL ends without trailing slash
        api_url = api_url.rstrip('/')
        processes_url = f"{api_url}/processes"
        
        response = requests.get(processes_url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        processes = data.get('processes', [])
        
        # Extract process names
        process_names = {proc.get('id', '') for proc in processes if proc.get('id')}
        
        return {
            'success': True,
            'processes': process_names,
            'total_count': len(process_names),
            'response_time': response.elapsed.total_seconds(),
            'status_code': response.status_code,
            'raw_response': data  # Include the raw /processes endpoint response
        }
        
    except requests.exceptions.Timeout:
        return {'success': False, 'error': 'Request timeout (30s)', 'error_type': 'timeout'}
    except requests.exceptions.ConnectionError:
        return {'success': False, 'error': 'Connection error', 'error_type': 'connection'}
    except requests.exceptions.HTTPError as e:
        return {'success': False, 'error': f'HTTP error: {e.response.status_code}', 'error_type': 'http'}
    except json.JSONDecodeError:
        return {'success': False, 'error': 'Invalid JSON response', 'error_type': 'json'}
    except Exception as e:
        return {'success': False, 'error': f'Unexpected error: {str(e)}', 'error_type': 'unknown'}

def check_profile_compliance(backend_processes: Set[str], profile_processes: Set[str]) -> Dict:
    """
    Check compliance of backend processes against a profile.
    
    Args:
        backend_processes: Set of process names from backend
        profile_processes: Set of required process names for profile
        
    Returns:
        Dictionary with compliance statistics
    """
    available = backend_processes.intersection(profile_processes)
    missing = profile_processes - backend_processes
    
    compliance_rate = len(available) / len(profile_processes) if profile_processes else 0
    
    return {
        'total_required': len(profile_processes),
        'available': len(available),
        'missing': len(missing),
        'compliance_rate': compliance_rate,
        'missing_processes': list(missing),
        'available_processes': list(available)
    }

def check_profile_compliance_detailed(backend_processes: Set[str], profile_info: Dict) -> Dict:
    """
    Check compliance with detailed experimental process tracking.
    
    Args:
        backend_processes: Set of process names from backend
        profile_info: Dictionary with 'processes', 'experimental', 'stable' sets
        
    Returns:
        Dictionary with detailed compliance statistics including experimental tracking
    """
    all_profile_processes = profile_info['processes']
    experimental_processes = profile_info['experimental']
    stable_processes = profile_info['stable']
    
    # Overall compliance
    available = backend_processes.intersection(all_profile_processes)
    missing = all_profile_processes - backend_processes
    
    # Stable process compliance
    stable_available = backend_processes.intersection(stable_processes)
    stable_missing = stable_processes - backend_processes
    
    # Experimental process compliance
    experimental_available = backend_processes.intersection(experimental_processes)
    experimental_missing = experimental_processes - backend_processes
    
    compliance_rate = len(available) / len(all_profile_processes) if all_profile_processes else 0
    stable_compliance_rate = len(stable_available) / len(stable_processes) if stable_processes else 0
    
    return {
        'total_required': len(all_profile_processes),
        'available': len(available),
        'missing': len(missing),
        'compliance_rate': compliance_rate,
        'missing_processes': list(missing),
        'available_processes': list(available),
        # Stable process details
        'stable_total': len(stable_processes),
        'stable_available': len(stable_available),
        'stable_missing': len(stable_missing),
        'stable_compliance_rate': stable_compliance_rate,
        'stable_missing_processes': list(stable_missing),
        'stable_available_processes': list(stable_available),
        # Experimental process details
        'experimental_total': len(experimental_processes),
        'experimental_available': len(experimental_available),
        'experimental_missing': len(experimental_missing),
        'experimental_missing_processes': list(experimental_missing),
        'experimental_available_processes': list(experimental_available),
    }

def check_backend_processes(backend_name: str, api_url: str) -> Dict:
    """
    Check process availability and compliance for a single backend.
    
    Args:
        backend_name: Name of the backend
        api_url: Base URL of the OpenEO backend
        
    Returns:
        Dictionary with complete compliance check results
    """
    result = {
        'backend': backend_name,
        'api_url': api_url,
        'timestamp': datetime.datetime.now().isoformat()
    }
    
    # Get processes from backend
    process_info = get_backend_processes(api_url)
    
    if not process_info['success']:
        result.update({
            'success': False,
            'error': process_info['error'],
            'error_type': process_info['error_type']
        })
        return result
    
    backend_processes = process_info['processes']
    result.update({
        'success': True,
        'total_processes': process_info['total_count'],
        'response_time': process_info['response_time'],
        'status_code': process_info['status_code']
    })
    
    # Check compliance for each profile (legacy format for backward compatibility)
    for profile_name, profile_processes in ALL_PROFILES.items():
        compliance = check_profile_compliance(backend_processes, profile_processes)
        result[f'{profile_name.lower()}_compliance'] = compliance
    
    # Check detailed compliance with experimental tracking
    for profile_name, profile_info in DETAILED_PROFILES.items():
        detailed_compliance = check_profile_compliance_detailed(backend_processes, profile_info)
        result[f'{profile_name.lower()}_detailed_compliance'] = detailed_compliance
    
    # Overall compliance across all profiles
    all_required_processes = set()
    for profile_processes in ALL_PROFILES.values():
        all_required_processes.update(profile_processes)
    
    overall_compliance = check_profile_compliance(backend_processes, all_required_processes)
    result['overall_compliance'] = overall_compliance
    
    # Custom processes (not in any standard profile)
    custom_processes = backend_processes - all_required_processes
    result['custom_compliance'] = {
        'total_required': 0,  # No requirement for custom processes
        'available': len(custom_processes),
        'missing': 0,
        'compliance_rate': 1.0 if custom_processes else 0.0,
        'missing_processes': [],
        'available_processes': list(custom_processes)
    }
    
    return result

def write_process_details_csv(backend_processes: Set[str], backend_name: str, api_url: str, output_file: str, backend_process_details: Dict = None):
    """
    Write detailed process information to CSV with individual process rows.
    
    Args:
        backend_processes: Set of process names from backend
        backend_name: Name of the backend
        api_url: API URL of the backend
        output_file: Path to output CSV file
        backend_process_details: Full process details from backend /processes endpoint
    """
    fieldnames = ['process', 'level', 'status', 'compatibility', 'reason', 'experimental']
    
    # Load official OpenEO process specifications for comparison
    official_specs = load_official_process_specs()

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Check each process in each profile level
        for profile_name, profile_processes in ALL_PROFILES.items():
            level = profile_name.lower()
            
            for process_name in sorted(profile_processes):
                status = 'available' if process_name in backend_processes else 'not_available'
                
                # Check if process is experimental in backend
                experimental = 'no'  # Default to no
                if status == 'available' and backend_process_details:
                    # Find the backend process details
                    backend_process = None
                    for process in backend_process_details.get('processes', []):
                        if process.get('id') == process_name:
                            backend_process = process
                            break
                    
                    if backend_process and backend_process.get('experimental', False):
                        experimental = 'yes'
                
                # Determine compatibility through parameter comparison
                if status == 'available' and backend_process_details and process_name in official_specs:
                    # Find the backend process details (reuse logic from above)
                    backend_process = None
                    for process in backend_process_details.get('processes', []):
                        if process.get('id') == process_name:
                            backend_process = process
                            break
                    
                    if backend_process:
                        is_compatible, reason = compare_process_schemas(
                            backend_process, 
                            official_specs[process_name]
                        )
                        compatibility = 'compatible' if is_compatible else 'mismatch'
                    else:
                        compatibility = 'unknown'
                        reason = 'Process details not found in backend response'
                elif status == 'available':
                    # Available but can't compare (no official spec or details)
                    compatibility = 'unknown'
                    reason = 'Cannot verify compatibility - missing specification'
                else:
                    compatibility = 'unknown'
                    reason = 'Process not available in backend'
                
                writer.writerow({
                    'process': process_name,
                    'level': level,
                    'status': status,
                    'compatibility': compatibility,
                    'reason': reason,
                    'experimental': experimental
                })
        
        # Also check for processes that exist in backend but not in any profile
        all_profile_processes = set()
        for profile_processes in ALL_PROFILES.values():
            all_profile_processes.update(profile_processes)
        
        extra_processes = backend_processes - all_profile_processes
        for process_name in sorted(extra_processes):
            # Check if custom process is experimental
            experimental = 'no'  # Default to no
            if backend_process_details:
                backend_process = None
                for process in backend_process_details.get('processes', []):
                    if process.get('id') == process_name:
                        backend_process = process
                        break
                
                if backend_process and backend_process.get('experimental', False):
                    experimental = 'yes'
            
            writer.writerow({
                'process': process_name,
                'level': 'custom',
                'status': 'available',
                'compatibility': 'unknown',
                'reason': 'Process not in any standard profile',
                'experimental': experimental
            })

def write_raw_processes_json(api_url: str, output_file: str):
    """
    Fetch and write the raw /processes endpoint response to a JSON file.
    
    Args:
        api_url: Base URL of the OpenEO backend
        output_file: Path to output JSON file
    """
    try:
        # Ensure URL ends without trailing slash
        api_url = api_url.rstrip('/')
        processes_url = f"{api_url}/processes"
        
        response = requests.get(processes_url, timeout=30)
        response.raise_for_status()
        
        # Write the raw response directly to the JSON file
        raw_data = response.json()
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, indent=2)
            
    except Exception as e:
        # If we can't fetch the raw data, write an error message
        error_data = {
            "error": f"Failed to fetch /processes endpoint: {str(e)}",
            "url": f"{api_url}/processes"
        }
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(error_data, f, indent=2)

def process_single_backend(backend_name: str, api_url: str, output_file: str, output_format: str = 'detailed'):
    """
    Check processes for a single backend and save results.
    Always generates both CSV and JSON outputs.
    
    Args:
        backend_name: Name of the backend
        api_url: Base URL of the OpenEO backend  
        output_file: Base path for output files (without extension)
        output_format: Output format ('detailed', 'summary') - kept for compatibility
    """
    result = check_backend_processes(backend_name, api_url)
    
    if not result['success']:
        print(f"✗ {backend_name}: {result['error']}")
        return
    
    backend_processes = get_backend_processes(api_url)['processes']
    
    # Fetch full process details for parameter comparison
    backend_process_details = None
    try:
        api_url_clean = api_url.rstrip('/')
        processes_url = f"{api_url_clean}/processes"
        response = requests.get(processes_url, timeout=30)
        response.raise_for_status()
        backend_process_details = response.json()
    except Exception as e:
        print(f"Warning: Could not fetch process details for parameter comparison: {e}")
    
    # Generate base filename without extension
    if output_file.endswith('.csv') or output_file.endswith('.json'):
        base_filename = output_file.rsplit('.', 1)[0]
    else:
        base_filename = output_file
    
    # Always write both CSV and JSON files
    csv_file = f"{base_filename}.csv"
    json_file = f"{base_filename}.json"
    
    # Write CSV with process,level,status,compatibility,reason format
    write_process_details_csv(backend_processes, backend_name, api_url, csv_file, backend_process_details)
    
    # Write raw /processes endpoint response to JSON
    write_raw_processes_json(api_url, json_file)
    
    # Print summary with experimental process details
    print(f"✓ {backend_name}: {result['total_processes']} processes available")
    for profile in ['L1', 'L2', 'L3', 'L4']:
        compliance = result[f'{profile.lower()}_compliance']
        detailed = result[f'{profile.lower()}_detailed_compliance']
        
        total_available = compliance['available']
        total_missing = compliance['missing']
        total_required = compliance['total_required']
        rate = compliance['compliance_rate'] * 100
        
        # Count experimental processes within the available ones
        experimental_in_available = detailed['experimental_available']
        
        # Format: Available (Experimental_in_available, Missing)
        print(f"  {profile}: {total_available}/{total_required} ({rate:.1f}%) [{total_available} ({experimental_in_available}, {total_missing})]")
    
    print("Generated files:")
    print(f"  CSV: {csv_file}")
    print(f"  JSON: {json_file}")

def process_backends_from_csv(input_csv: str, output_file: str, output_format: str = 'summary'):
    """
    Check processes for multiple backends from CSV file.
    Always generates both CSV summary and JSON files.
    
    Args:
        input_csv: Path to CSV file with backend information
        output_file: Base path for output files (without extension)
        output_format: Output format ('detailed', 'summary') - kept for compatibility
    """
    results = []
    
    try:
        with open(input_csv, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                backend_name = row.get('backend', row.get('name', 'unknown'))
                api_url = row.get('api_url', row.get('url', ''))
                
                if not api_url:
                    continue
                    
                print(f"Checking {backend_name}...")
                result = check_backend_processes(backend_name, api_url)
                results.append(result)
                
                # Print summary
                if result['success']:
                    print(f"✓ {backend_name}: {result['total_processes']} processes")
                else:
                    print(f"✗ {backend_name}: {result['error']}")
    
    except FileNotFoundError:
        print(f"Error: Input file '{input_csv}' not found")
        return
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    # Generate base filename without extension
    if output_file.endswith('.csv') or output_file.endswith('.json'):
        base_filename = output_file.rsplit('.', 1)[0]
    else:
        base_filename = output_file
    
    # Always write both CSV and JSON files
    csv_file = f"{base_filename}.csv"
    json_file = f"{base_filename}.json"
    
    # Write CSV with summary format (compatible with process_summary.py)
    write_results_to_csv(results, csv_file)
    
    # Write JSON with all results
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    
    print("Generated files:")
    print(f"  CSV: {csv_file}")
    print(f"  JSON: {json_file}")
    print(f"Processed {len(results)} backends")
    
    print(f"\nResults saved to: {output_file}")

def write_results_to_csv(results: List[Dict], output_file: str):
    """
    Write process check results to CSV file.
    
    Args:
        results: List of result dictionaries
        output_file: Path to output CSV file
    """
    if not results:
        return
    
    fieldnames = [
        'backend', 'api_url', 'timestamp', 'success', 'total_processes',
        'response_time', 'status_code', 'error', 'error_type',
        'l1_available', 'l1_total', 'l1_compliance_rate',
        'l2_available', 'l2_total', 'l2_compliance_rate', 
        'l3_available', 'l3_total', 'l3_compliance_rate',
        'l4_available', 'l4_total', 'l4_compliance_rate',
        'custom', 'custom_total', 'custom_compliance_rate',
        'overall_available', 'overall_total', 'overall_compliance_rate'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for result in results:
            row = {
                'backend': result.get('backend', ''),
                'api_url': result.get('api_url', ''),
                'timestamp': result.get('timestamp', ''),
                'success': result.get('success', False),
                'total_processes': result.get('total_processes', 0),
                'response_time': result.get('response_time', 0),
                'status_code': result.get('status_code', ''),
                'error': result.get('error', ''),
                'error_type': result.get('error_type', '')
            }
            
            # Add compliance data for each profile
            for profile in ['l1', 'l2', 'l3', 'l4', 'overall']:
                compliance_key = f'{profile}_compliance'
                if compliance_key in result:
                    compliance = result[compliance_key]
                    row[f'{profile}_available'] = compliance.get('available', 0)
                    row[f'{profile}_total'] = compliance.get('total_required', 0)
                    row[f'{profile}_compliance_rate'] = compliance.get('compliance_rate', 0)
                else:
                    row[f'{profile}_available'] = 0
                    row[f'{profile}_total'] = 0
                    row[f'{profile}_compliance_rate'] = 0
            
            # Handle custom processes separately with simplified column name
            if 'custom_compliance' in result:
                custom_compliance = result['custom_compliance']
                row['custom'] = custom_compliance.get('available', 0)
                row['custom_total'] = custom_compliance.get('total_required', 0)
                row['custom_compliance_rate'] = custom_compliance.get('compliance_rate', 0)
            else:
                row['custom'] = 0
                row['custom_total'] = 0
                row['custom_compliance_rate'] = 0
            
            writer.writerow(row)

def load_official_process_specs(spec_file: str = "openeo_processes_1.0.json") -> Dict[str, Dict]:
    """
    Load the official OpenEO 1.0 process specifications.
    
    Args:
        spec_file: Path to the JSON file containing official OpenEO process specifications
        
    Returns:
        Dictionary mapping process IDs to their complete specification
    """
    try:
        with open(spec_file, 'r') as f:
            processes = json.load(f)
        
        # Convert list to dictionary keyed by process ID
        spec_dict = {}
        for process in processes:
            if 'id' in process:
                spec_dict[process['id']] = process
        
        return spec_dict
    except FileNotFoundError:
        print(f"Warning: Official process specification file '{spec_file}' not found")
        return {}
    except json.JSONDecodeError as e:
        print(f"Warning: Error parsing official process specification: {e}")
        return {}

def compare_process_schemas(backend_process: Dict, official_process: Dict) -> Tuple[bool, str]:
    """
    Compare a backend process schema with the official OpenEO specification.
    
    Args:
        backend_process: Process definition from backend /processes endpoint
        official_process: Official process definition from OpenEO 1.0 spec
        
    Returns:
        Tuple of (is_compatible, reason_if_incompatible)
    """
    issues = []
    
    # Compare parameters
    backend_params = {p['name']: p for p in backend_process.get('parameters', [])}
    official_params = {p['name']: p for p in official_process.get('parameters', [])}
    
    # Check for missing required parameters
    for param_name, param_spec in official_params.items():
        if param_name not in backend_params:
            # Check if parameter is required (default is True unless explicitly optional)
            is_optional = param_spec.get('optional', False)
            if not is_optional:
                issues.append(f"Missing required parameter '{param_name}'")
        else:
            # Compare parameter schemas if both exist
            backend_param = backend_params[param_name]
            schema_issues = compare_parameter_schemas(backend_param, param_spec, param_name)
            issues.extend(schema_issues)
    
    # Check for extra parameters (not in official spec)
    for param_name in backend_params:
        if param_name not in official_params:
            # Check if the backend parameter is optional - if so, skip mismatch
            backend_param = backend_params[param_name]
            is_backend_optional = backend_param.get('optional', False)
            if not is_backend_optional:
                issues.append(f"Extra parameter '{param_name}' not in official spec")
    
    # Compare return type
    backend_returns = backend_process.get('returns', {})
    official_returns = official_process.get('returns', {})
    if backend_returns and official_returns:
        return_issues = compare_return_schemas(backend_returns, official_returns)
        issues.extend(return_issues)
    
    # Determine compatibility
    is_compatible = len(issues) == 0
    reason = "; ".join(issues) if issues else ""
    
    return is_compatible, reason

def compare_parameter_schemas(backend_param: Dict, official_param: Dict, param_name: str) -> List[str]:
    """
    Compare parameter schemas between backend and official specification.
    
    Args:
        backend_param: Parameter definition from backend
        official_param: Parameter definition from official spec
        param_name: Name of the parameter for error reporting
        
    Returns:
        List of compatibility issues
    """
    issues = []
    
    backend_schema = backend_param.get('schema', {})
    official_schema = official_param.get('schema', {})
    
    # Handle case where backend schema might be an array of type definitions (union type)
    if isinstance(backend_schema, list):
        # Backend uses array format for union types: [{"type": "number"}, {"type": "string"}]
        backend_types = []
        for schema_item in backend_schema:
            if isinstance(schema_item, dict) and 'type' in schema_item:
                item_type = schema_item['type']
                if isinstance(item_type, list):
                    backend_types.extend(item_type)
                else:
                    backend_types.append(item_type)
    elif isinstance(backend_schema, dict):
        # Standard format: {"type": ["number", "string"]} or {"type": "number"}
        backend_types = backend_schema.get('type', [])
    else:
        issues.append(f"Parameter '{param_name}' has unsupported schema format in backend")
        return issues
    
    if not isinstance(official_schema, dict):
        # Can't compare against invalid official schema
        return issues
    
    # Get official types
    official_types = official_schema.get('type', [])
    
    # Normalize types to lists for comparison
    if isinstance(backend_types, str):
        backend_types = [backend_types]
    if isinstance(official_types, str):
        official_types = [official_types]
    
    # Check if backend supports all required types
    if official_types and backend_types:
        missing_types = set(official_types) - set(backend_types)
        if missing_types:
            issues.append(f"Parameter '{param_name}' missing types: {', '.join(missing_types)}")
    
    return issues

def compare_return_schemas(backend_returns: Dict, official_returns: Dict) -> List[str]:
    """
    Compare return schemas between backend and official specification.
    
    Args:
        backend_returns: Return definition from backend
        official_returns: Return definition from official spec
        
    Returns:
        List of compatibility issues
    """
    issues = []
    
    backend_schema = backend_returns.get('schema', {})
    official_schema = official_returns.get('schema', {})
    
    # Handle case where backend schema might be an array of type definitions (union type)
    if isinstance(backend_schema, list):
        # Backend uses array format for union types: [{"type": "number"}, {"type": "string"}]
        backend_types = []
        for schema_item in backend_schema:
            if isinstance(schema_item, dict) and 'type' in schema_item:
                item_type = schema_item['type']
                if isinstance(item_type, list):
                    backend_types.extend(item_type)
                else:
                    backend_types.append(item_type)
    elif isinstance(backend_schema, dict):
        # Standard format: {"type": ["number", "string"]} or {"type": "number"}
        backend_types = backend_schema.get('type', [])
    else:
        issues.append("Return schema has unsupported format in backend")
        return issues
    
    if not isinstance(official_schema, dict):
        return issues
    
    # Get official types
    official_types = official_schema.get('type', [])
    
    # Normalize types to lists for comparison
    if isinstance(backend_types, str):
        backend_types = [backend_types]
    if isinstance(official_types, str):
        official_types = [official_types]
    
    # Check if backend return type matches expected types
    if official_types and backend_types:
        if not any(t in backend_types for t in official_types):
            issues.append(f"Return type mismatch: expected {official_types}, got {backend_types}")
    
    return issues

def main():
    parser = argparse.ArgumentParser(
        description='Check OpenEO process availability and compliance',
        epilog="""
Examples:
  # Check single backend (detailed output)
  python process_checker.py --url "https://openeo.vito.be/openeo/1.1" -o results.csv
  
  # Check single backend (summary output for process-summary command)
  python process_checker.py --url "https://openeo.vito.be/openeo/1.1" -o results.csv --format summary
  
  # Check multiple backends from CSV (always summary format)
  python process_checker.py -i backends.csv -o process_compliance.csv
        """
    )
    
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('-i', '--input', help='Input CSV file with backend information')
    input_group.add_argument('--url', help='API URL for single backend check')
    
    parser.add_argument('-o', '--output', required=True, help='Output file (.csv or .json)')
    parser.add_argument('--format', choices=['detailed', 'summary'], default='detailed',
                      help='Output format: detailed (process-by-process) or summary (backend-level stats)')
    
    args = parser.parse_args()
    
    if args.url:
        # Extract backend name from URL for single URL mode
        from urllib.parse import urlparse
        parsed_url = urlparse(args.url)
        backend_name = parsed_url.netloc.split('.')[0] if parsed_url.netloc else 'unknown'
        process_single_backend(backend_name, args.url, args.output, args.format)
    else:
        # Multiple backends always use summary format (for compatibility with process_summary.py)
        process_backends_from_csv(args.input, args.output, 'summary')
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
