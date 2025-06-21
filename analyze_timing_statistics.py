#!/usr/bin/env python3
"""
OpenEO Backend Timing Statistics Analyzer

This script analyzes timing statistics from OpenEO backend operations,
calculating detailed metrics about each phase of processing, including:
- Time to submission
- Time to enter in the queue
- Time to start processing
- Time to finish processing
- Downloading time

Usage:
    python analyze_timing_statistics.py [--input INPUT_DIR] [--output OUTPUT_DIR]
                                        [--backend BACKEND] [--format FORMAT]
                                        [--location LOCATION] [--bbox-size BBOX_SIZE]
"""

import os
import json
import argparse
import logging
import re
import datetime
import glob
from typing import Dict, List, Any
from collections import defaultdict
import statistics

# Try to import optional dependencies
try:
    import matplotlib.pyplot as plt
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("Warning: matplotlib not available, visualizations will be skipped")

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Warning: pandas not available, some features will be limited")

try:
    import seaborn as sns
    SEABORN_AVAILABLE = True
except ImportError:
    SEABORN_AVAILABLE = False
    print("Warning: seaborn not available, some visualizations will be simplified")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def find_result_files(input_dir: str) -> List[str]:
    """
    Find all result files in the input directory structure.
    
    Args:
        input_dir: Directory containing OpenEO test results
        
    Returns:
        List of paths to result JSON files
    """
    # Look for comprehensive result files (scenario_name_timestamp_results.json)
    result_files = glob.glob(os.path.join(input_dir, "**", "*_results.json"), recursive=True)
    
    # If no result files found, look for legacy job_result.json files
    if not result_files:
        result_files = glob.glob(os.path.join(input_dir, "**", "job_result.json"), recursive=True)
        
        # Also check for scenario_results.json files
        scenario_results = glob.glob(os.path.join(input_dir, "**", "scenario_results.json"), recursive=True)
        result_files.extend(scenario_results)
    
    logger.info(f"Found {len(result_files)} result files in {input_dir}")
    return result_files


def load_result_data(result_file: str) -> Dict[str, Any]:
    """
    Load result data from a file.
    
    Args:
        result_file: Path to the result file
        
    Returns:
        Dictionary containing result data
    """
    try:
        with open(result_file, 'r') as file:
            data = json.load(file)
        
        # If this is a comprehensive result file (from updated openeotest.py)
        if isinstance(data, dict) and "start_time" in data and "job_status_history" in data:
            return data
            
        # If this is a legacy job_result.json, try to load the companion job_logs.json
        if isinstance(data, dict) and set(data.keys()) == {"backend_url", "process_graph", "job_id", "status"}:
            # Try to find and load the companion job_logs.json
            logs_file = os.path.join(os.path.dirname(result_file), "job_logs.json")
            if os.path.exists(logs_file):
                with open(logs_file, 'r') as logs_f:
                    logs_data = json.load(logs_f)
                # Merge the data
                return {**data, **logs_data}
            
        return data
    except Exception as e:
        logger.error(f"Error loading result file {result_file}: {e}")
        return {}


def parse_log_file(log_file: str) -> Dict[str, Any]:
    """
    Parse timing information from a log file.
    
    Args:
        log_file: Path to the log file
        
    Returns:
        Dictionary containing extracted timing information
    """
    if not os.path.exists(log_file):
        logger.warning(f"Log file not found: {log_file}")
        return {}
    
    timing_info = {
        "job_creation_time": None,
        "queue_time": None,
        "processing_time": None,
        "download_time": None,
        "total_time": None,
        "status_changes": {},
        "download_stats": {
            "file_count": 0,
            "total_size_mb": 0.0
        }
    }
    
    try:
        with open(log_file, 'r') as file:
            content = file.read()
            
            # Extract job ID
            job_id_match = re.search(r"Job created with ID: (.*?)$", content, re.MULTILINE)
            if job_id_match:
                timing_info["job_id"] = job_id_match.group(1).strip()
            
            # Extract status change times
            status_changes = re.findall(r"Job status changed to '(\w+)' after ([\d.]+) seconds", content)
            for status, time_str in status_changes:
                timing_info["status_changes"][status] = float(time_str)
            
            # Extract queue time
            queue_match = re.search(r"Job was queued for ([\d.]+) seconds", content)
            if queue_match:
                timing_info["queue_time"] = float(queue_match.group(1))
            
            # Extract processing time
            processing_match = re.search(r"Backend processing time: ([\d.]+) seconds", content)
            if processing_match:
                timing_info["processing_time"] = float(processing_match.group(1))
            
            # Extract download time
            download_match = re.search(r"Download time: ([\d.]+) seconds", content)
            if download_match:
                timing_info["download_time"] = float(download_match.group(1))
            
            # Extract total time
            total_match = re.search(r"Total processing time: ([\d.]+) seconds", content)
            if total_match:
                timing_info["total_time"] = float(total_match.group(1))
            
            # Extract download stats
            file_count_match = re.search(r"Files downloaded: (\d+)", content)
            if file_count_match:
                timing_info["download_stats"]["file_count"] = int(file_count_match.group(1))
            
            size_match = re.search(r"Total size: ([\d.]+) MB", content)
            if size_match:
                timing_info["download_stats"]["total_size_mb"] = float(size_match.group(1))
            
        return timing_info
    except Exception as e:
        logger.error(f"Error parsing log file {log_file}: {e}")
        return {}


def extract_detailed_phase_timings(results: List[Dict[str, Any]]) -> Dict[str, List[float]]:
    """
    Extract detailed phase timings from job status history.
    
    Args:
        results: List of job result dictionaries
        
    Returns:
        Dictionary with lists of timing values for each phase
    """
    phase_timings = {
        "time_to_submission": [],  # Time from start to job submission
        "time_to_queue": [],       # Time from submission to queued
        "time_to_processing": [],  # Time from queued to running
        "time_to_finish": [],      # Time from running to finished
        "time_to_download": []     # Time for downloading results
    }
    
    for result in results:
        # Check if job_status_history exists
        if "job_status_history" in result and isinstance(result["job_status_history"], dict):
            history = result["job_status_history"]
            
            # Convert timestamps to datetime objects
            timestamps = {}
            for status, timestamp_str in history.items():
                try:
                    timestamps[status] = datetime.datetime.fromisoformat(timestamp_str)
                except (ValueError, TypeError):
                    continue
            
            # Calculate phase timings if timestamps are available
            if "submitted" in timestamps:
                # Time to submission (if start_time is available)
                if "start_time" in result:
                    try:
                        start_time = datetime.datetime.fromtimestamp(result["start_time"])
                        submission_delta = (timestamps["submitted"] - start_time).total_seconds()
                        phase_timings["time_to_submission"].append(submission_delta)
                    except (ValueError, TypeError, OSError):
                        pass
                        
                # Time to queue
                if "queued" in timestamps:
                    delta = (timestamps["queued"] - timestamps["submitted"]).total_seconds()
                    phase_timings["time_to_queue"].append(delta)
                    
                    # Time to processing
                    if "running" in timestamps:
                        delta = (timestamps["running"] - timestamps["queued"]).total_seconds()
                        phase_timings["time_to_processing"].append(delta)
                        
                        # Time to finish
                        if "finished" in timestamps:
                            delta = (timestamps["finished"] - timestamps["running"]).total_seconds()
                            phase_timings["time_to_finish"].append(delta)
        
        # Add download time if available
        if "download_time" in result and result["download_time"] is not None:
            phase_timings["time_to_download"].append(result["download_time"])
    
    return phase_timings


def calculate_timing_statistics(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate timing statistics from a list of job results.
    
    Args:
        results: List of job result dictionaries
        
    Returns:
        Dictionary containing timing statistics
    """
    stats = {}
    
    # Skip if no results
    if not results:
        return {
            "error": "No results found"
        }
    
    # Count success vs. failed jobs
    successful = [r for r in results if r.get("status") == "success" and r.get("job_status") == "finished"]
    
    stats["total_jobs"] = len(results)
    stats["successful_jobs"] = len(successful)
    stats["failed_jobs"] = len(results) - len(successful)
    stats["success_rate"] = len(successful) / len(results) if results else 0
    
    # Calculate statistics for successful jobs
    if not successful:
        return {
            **stats,
            "error": "No successful jobs found"
        }
    
    # Collect all timing metrics
    submission_times = [r.get("submit_time") for r in successful if r.get("submit_time")]
    queue_times = [r.get("queue_time") for r in successful if r.get("queue_time") is not None]
    processing_times = [r.get("processing_time") for r in successful if r.get("processing_time") is not None]
    download_times = [r.get("download_time") for r in successful if r.get("download_time")]
    total_times = [r.get("total_time") for r in successful if r.get("total_time")]
    
    # Extract detailed phase timings from job_status_history
    detailed_phase_timings = extract_detailed_phase_timings(successful)
    
    # Calculate statistics for each timing metric
    def calculate_metric_stats(values):
        if not values:
            return {"error": "No data available"}
        
        return {
            "min": min(values),
            "max": max(values),
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "std_dev": statistics.stdev(values) if len(values) > 1 else 0,
            "count": len(values)
        }
    
    stats["submission_time"] = calculate_metric_stats(submission_times)
    stats["queue_time"] = calculate_metric_stats(queue_times)
    stats["processing_time"] = calculate_metric_stats(processing_times)
    stats["download_time"] = calculate_metric_stats(download_times)
    stats["total_time"] = calculate_metric_stats(total_times)
    
    # Add detailed phase timing statistics
    stats["detailed_phases"] = {}
    for phase, times in detailed_phase_timings.items():
        if times:
            stats["detailed_phases"][phase] = calculate_metric_stats(times)
    
    # Calculate download statistics
    file_counts = [
        r.get("file_count", 0) for r in successful 
        if r.get("file_count", 0) > 0
    ]
    
    total_sizes = [
        r.get("total_size_mb", 0) for r in successful 
        if r.get("total_size_mb", 0) > 0
    ]
    
    if file_counts:
        stats["download_stats"] = {
            "total_files": sum(file_counts),
            "avg_files_per_job": sum(file_counts) / len(file_counts),
            "total_size_mb": sum(total_sizes) if total_sizes else 0,
            "avg_size_per_job_mb": sum(total_sizes) / len(total_sizes) if total_sizes else 0
        }
    
    return stats


def analyze_logs(input_dir: str, filters: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Analyze logs and calculate timing statistics.
    
    Args:
        input_dir: Directory containing OpenEO test results
        filters: Dictionary of filters (backend, format, location, bbox_size)
        
    Returns:
        Dictionary containing timing statistics and analysis
    """
    filters = filters or {}
    
    try:
        # Find all result files
        result_files = find_result_files(input_dir)
        
        # Load result data from each file
        all_results = []
        for result_file in result_files:
            data = load_result_data(result_file)
            if data:
                # Extract backend name from result data or filename
                if "backend_url" in data and "backend_name" not in data:
                    # Try to parse backend name from URL
                    backend_url = data["backend_url"]
                    try:
                        import urllib.parse
                        hostname = urllib.parse.urlparse(backend_url).netloc
                        data["backend_name"] = hostname.replace('.', '_')
                    except Exception:
                        data["backend_name"] = "unknown"
                
                # Make sure we have a status
                if "status" not in data and "job_status" in data:
                    data["status"] = "success" if data["job_status"] == "finished" else "failed"
                
                # Add the file path for reference
                data["result_file"] = result_file
                
                all_results.append(data)
        
        logger.info(f"Loaded {len(all_results)} valid results")
        
        # Apply filters if specified
        filtered_results = all_results
        if filters:
            for key, value in filters.items():
                if value:
                    filtered_results = [r for r in filtered_results if r.get(key) == value]
            
            logger.info(f"After filtering, {len(filtered_results)} results remain")
        
        # Sort results for easier analysis
        filtered_results.sort(key=lambda r: r.get("start_time", 0))
        
        # Group by backend, location, bbox_size, and format
        grouped_results = defaultdict(list)
        for result in filtered_results:
            backend = result.get("backend", "unknown")
            location = result.get("location", "unknown")
            bbox_size = result.get("bbox_size", "unknown")
            file_format = result.get("format", "unknown")
            
            key = f"{backend}_{location}_{bbox_size}_{file_format}"
            grouped_results[key].append(result)
        
        # Process logs to get additional timing info
        timing_details = {}
        for result in filtered_results:
            log_file = result.get("log_file")
            if log_file:
                # Ensure we have the correct path to the log file
                if log_file.startswith("../logs/"):
                    log_file = os.path.join(input_dir, log_file.replace("../", ""))
                elif not os.path.isabs(log_file):
                    log_file = os.path.join(input_dir, log_file)
                    
                if not os.path.exists(log_file):
                    # Try to find the log file in the logs directory
                    logs_dir = os.path.join(input_dir, "logs")
                    if os.path.exists(logs_dir):
                        base_log = os.path.basename(log_file)
                        potential_log = os.path.join(logs_dir, base_log)
                        if os.path.exists(potential_log):
                            log_file = potential_log
                
                if os.path.exists(log_file):
                    job_id = result.get("job_id")
                    if job_id:
                        timing_details[job_id] = parse_log_file(log_file)
        
        # Calculate statistics for each group
        group_statistics = {}
        for group_key, group_results in grouped_results.items():
            group_statistics[group_key] = calculate_timing_statistics(group_results)
        
        # Calculate overall statistics
        overall_statistics = calculate_timing_statistics(filtered_results)
        
        return {
            "timestamp": datetime.datetime.now().isoformat(),
            "filters": filters,
            "overall": overall_statistics,
            "by_backend_location_size_format": group_statistics,
            "timing_details": timing_details
        }
    
    except Exception as e:
        logger.error(f"Error analyzing logs: {e}")
        return {"error": str(e)}


def create_phase_proportion_visualization(stats: Dict[str, Any], output_dir: str) -> str:
    """
    Create a stacked bar chart showing the proportion of time spent in each processing phase.
    
    Args:
        stats: Dictionary of timing statistics
        output_dir: Directory to save visualization
        
    Returns:
        Path to the visualization file
    """
    if "by_backend_location_size_format" not in stats:
        return None
        
    try:
        # Collect phase data by backend
        backend_phases = {}
        
        for key, value in stats["by_backend_location_size_format"].items():
            if not isinstance(value, dict):
                continue
                
            backend = key.split('_')[0]
            
            # Skip if this backend is not in the result yet
            if backend not in backend_phases:
                backend_phases[backend] = {
                    "submission_time": 0,
                    "queue_time": 0,
                    "processing_time": 0,
                    "download_time": 0,
                    "count": 0
                }
            
            # Add phase times if available
            phases = ["submission_time", "queue_time", "processing_time", "download_time"]
            
            for phase in phases:
                if phase in value and "mean" in value[phase]:
                    backend_phases[backend][phase] += value[phase]["mean"]
                    
            # Increment count
            backend_phases[backend]["count"] += 1
        
        # Calculate averages
        for backend in backend_phases:
            count = backend_phases[backend]["count"]
            if count > 0:
                for phase in phases:
                    backend_phases[backend][phase] /= count
        
        # Create DataFrame for plotting
        df_data = []
        
        for backend, phase_data in backend_phases.items():
            for phase in phases:
                df_data.append({
                    "Backend": backend,
                    "Phase": phase.replace("_time", "").capitalize(),
                    "Time (seconds)": phase_data[phase]
                })
        
        # Create DataFrame
        import pandas as pd
        df = pd.DataFrame(df_data)
        
        # Pivot for stacked bar chart
        pivot_df = df.pivot(index="Backend", columns="Phase", values="Time (seconds)")
        
        # Create visualization
        fig, ax = plt.subplots(figsize=(12, 8))
        pivot_df.plot(kind="bar", stacked=True, ax=ax, colormap="viridis")
        
        # Add total time labels on top
        for i, backend in enumerate(pivot_df.index):
            total_time = pivot_df.loc[backend].sum()
            ax.text(i, total_time + 2, f"{total_time:.1f}s", ha="center")
        
        ax.set_title("Proportion of Time Spent in Each Processing Phase")
        ax.set_ylabel("Time (seconds)")
        ax.set_xlabel("Backend")
        plt.legend(title="Processing Phase")
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        plt.tight_layout()
        
        # Save figure
        fig_path = os.path.join(output_dir, "phase_proportions.png")
        plt.savefig(fig_path, dpi=300)
        plt.close(fig)
        
        return fig_path
        
    except Exception as e:
        logger.error(f"Error creating phase proportion visualization: {e}")
        return None


def create_job_status_timing_visualization(stats: Dict[str, Any], output_dir: str) -> str:
    """
    Create a visualization showing job status transition times.
    
    Args:
        stats: Dictionary of timing statistics with timing_details
        output_dir: Directory to save visualization
        
    Returns:
        Path to the visualization file
    """
    if "timing_details" not in stats or not stats["timing_details"]:
        return None
        
    try:
        # Collect status transition times
        status_times = defaultdict(list)
        
        for job_id, details in stats["timing_details"].items():
            status_changes = details.get("status_changes", {})
            
            # Skip if no status changes
            if not status_changes:
                continue
            
            # Extract times for different status transitions
            if "queued" in status_changes:
                status_times["submitted_to_queued"].append(status_changes["queued"])
                
            if "running" in status_changes and "queued" in status_changes:
                queue_time = status_changes["running"] - status_changes["queued"]
                status_times["queued_to_running"].append(queue_time)
                
            if "finished" in status_changes and "running" in status_changes:
                processing_time = status_changes["finished"] - status_changes["running"]
                status_times["running_to_finished"].append(processing_time)
        
        # Skip if no status transition times
        if not status_times:
            return None
        
        # Create DataFrame for box plot
        import pandas as pd
        plot_data = []
        
        for status, times in status_times.items():
            for time in times:
                plot_data.append({
                    "Transition": status.replace("_", " to ").title(),
                    "Time (seconds)": time
                })
        
        # Create DataFrame
        df = pd.DataFrame(plot_data)
        
        # Create visualization
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Use seaborn for a nicer boxplot
        import seaborn as sns
        sns.boxplot(x="Transition", y="Time (seconds)", data=df, palette="viridis", ax=ax)
        
        # Add individual data points for better visualization
        sns.stripplot(x="Transition", y="Time (seconds)", data=df, 
                     color="black", size=4, alpha=0.5, jitter=True, ax=ax)
        
        ax.set_title("Job Status Transition Times")
        ax.set_ylabel("Time (seconds)")
        ax.set_xlabel("Status Transition")
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        plt.tight_layout()
        
        # Save figure
        fig_path = os.path.join(output_dir, "status_transition_times.png")
        plt.savefig(fig_path, dpi=300)
        plt.close(fig)
        
        return fig_path
        
    except Exception as e:
        logger.error(f"Error creating job status timing visualization: {e}")
        return None


def create_detailed_phase_breakdown_visualization(stats: Dict[str, Any], output_dir: str) -> str:
    """
    Create a pie chart visualization showing the proportion of time spent in each phase.
    
    Args:
        stats: Dictionary of timing statistics
        output_dir: Directory to save visualization
        
    Returns:
        Path to the visualization file
    """
    if "overall" not in stats or "error" in stats["overall"]:
        return None
        
    try:
        # Extract phase data
        overall = stats["overall"]
        phases = [
            ("submission_time", "Job Submission"),
            ("queue_time", "Queue Time"),
            ("processing_time", "Processing"),
            ("download_time", "Download")
        ]
        
        # Collect phase data
        phase_data = []
        phase_labels = []
        
        for phase_key, phase_label in phases:
            if phase_key in overall and "mean" in overall[phase_key]:
                phase_data.append(overall[phase_key]["mean"])
                phase_labels.append(phase_label)
        
        # Skip if not enough data
        if not phase_data:
            return None
            
        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 9))
        
        # Create pie chart
        ax1.pie(phase_data, labels=phase_labels, autopct='%1.1f%%', startangle=90,
               wedgeprops={'edgecolor': 'white'}, textprops={'fontsize': 14})
        ax1.set_title('Proportion of Time Spent in Each Phase', fontsize=16)
        ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
        
        # Create stacked bar for absolute time
        phases_df = pd.DataFrame({
            'Phase': phase_labels,
            'Time': phase_data
        })
        
        # Sort by time descending
        phases_df = phases_df.sort_values('Time', ascending=False)
        
        # Create horizontal bar chart
        bars = ax2.barh(range(len(phase_labels)), phases_df['Time'], color=plt.cm.viridis(np.linspace(0, 0.8, len(phases))))
        
        # Add labels and adjust
        ax2.set_yticks(range(len(phase_labels)))
        ax2.set_yticklabels(phases_df['Phase'])
        ax2.set_xlabel('Time (seconds)', fontsize=14)
        ax2.set_title('Absolute Time per Processing Phase', fontsize=16)
        
        # Add values to bars
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax2.text(width + 1, bar.get_y() + bar.get_height()/2, f"{width:.2f}s",
                    ha='left', va='center', fontsize=12)
        
        plt.tight_layout()
        
        # Save figure
        fig_path = os.path.join(output_dir, "phase_breakdown_detailed.png")
        plt.savefig(fig_path, dpi=300)
        plt.close(fig)
        
        return fig_path
        
    except Exception as e:
        logger.error(f"Error creating detailed phase breakdown visualization: {e}")
        return None


def create_timing_visualizations(stats: Dict[str, Any], output_dir: str) -> Dict[str, str]:
    """
    Create visualizations of timing statistics.
    
    Args:
        stats: Dictionary of timing statistics
        output_dir: Directory to save visualizations
        
    Returns:
        Dictionary mapping visualization types to file paths
    """
    visualization_paths = {}
    os.makedirs(output_dir, exist_ok=True)
    
    # Create overall timing visualization
    if "overall" in stats and stats["overall"].get("successful_jobs", 0) > 0:
        try:
            fig, ax = plt.subplots(figsize=(12, 6))
            
            metrics = [
                ("submission_time", "Job Submission"),
                ("queue_time", "Queue Time"),
                ("processing_time", "Processing"),
                ("download_time", "Download")
            ]
            
            data = []
            labels = []
            error_bars = []
            
            for metric_key, metric_label in metrics:
                metric_stats = stats["overall"].get(metric_key, {})
                if metric_stats and "mean" in metric_stats:
                    data.append(metric_stats["mean"])
                    labels.append(metric_label)
                    error_bars.append(metric_stats.get("std_dev", 0))
            
            x = np.arange(len(labels))
            width = 0.6
            
            bars = ax.bar(x, data, width, yerr=error_bars, capsize=10,
                         color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
            
            ax.set_ylabel('Time (seconds)')
            ax.set_title('Average Time per Processing Phase')
            ax.set_xticks(x)
            ax.set_xticklabels(labels)
            
            # Add values on top of bars
            for i, bar in enumerate(bars):
                height = bar.get_height()
                ax.annotate(f'{height:.2f}s',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3),
                           textcoords="offset points",
                           ha='center', va='bottom')
            
            plt.tight_layout()
            
            # Save figure
            fig_path = os.path.join(output_dir, "processing_phases.png")
            plt.savefig(fig_path, dpi=300)
            plt.close(fig)
            
            visualization_paths["processing_phases"] = fig_path
            logger.info(f"Created processing phases visualization at {fig_path}")
            
        except Exception as e:
            logger.error(f"Error creating timing visualization: {e}")
    
    # Create backend comparison visualization if multiple backends
    if "by_backend_location_size_format" in stats:
        try:
            # Group by backend
            backend_stats = defaultdict(list)
            
            for key, value in stats["by_backend_location_size_format"].items():
                if isinstance(value, dict) and "total_time" in value and "mean" in value["total_time"]:
                    backend = key.split('_')[0]
                    backend_stats[backend].append(value["total_time"]["mean"])
            
            if len(backend_stats) > 1:
                fig, ax = plt.subplots(figsize=(10, 6))
                
                backends = list(backend_stats.keys())
                data = [statistics.mean(times) if times else 0 for backend, times in backend_stats.items()]
                err = [statistics.stdev(times) if len(times) > 1 else 0 for backend, times in backend_stats.items()]
                
                bars = ax.bar(backends, data, yerr=err, capsize=10, color='#1f77b4')
                
                ax.set_ylabel('Average Total Time (seconds)')
                ax.set_title('Performance Comparison by Backend')
                
                # Add values on top of bars
                for i, bar in enumerate(bars):
                    height = bar.get_height()
                    ax.annotate(f'{height:.2f}s',
                               xy=(bar.get_x() + bar.get_width() / 2, height),
                               xytext=(0, 3),
                               textcoords="offset points",
                               ha='center', va='bottom')
                
                plt.tight_layout()
                
                # Save figure
                fig_path = os.path.join(output_dir, "backend_comparison.png")
                plt.savefig(fig_path, dpi=300)
                plt.close(fig)
                
                visualization_paths["backend_comparison"] = fig_path
                logger.info(f"Created backend comparison visualization at {fig_path}")
        
        except Exception as e:
            logger.error(f"Error creating backend comparison visualization: {e}")
    
    # Create phase proportion visualization
    try:
        phase_prop_path = create_phase_proportion_visualization(stats, output_dir)
        if phase_prop_path:
            visualization_paths["phase_proportions"] = phase_prop_path
            logger.info(f"Created phase proportion visualization at {phase_prop_path}")
    except Exception as e:
        logger.error(f"Error creating phase proportion visualization: {e}")
    
    # Create job status transition visualization
    try:
        status_timing_path = create_job_status_timing_visualization(stats, output_dir)
        if status_timing_path:
            visualization_paths["status_timing"] = status_timing_path
            logger.info(f"Created job status timing visualization at {status_timing_path}")
    except Exception as e:
        logger.error(f"Error creating job status timing visualization: {e}")
    
    # Create detailed phase breakdown visualization
    try:
        detailed_phase_path = create_detailed_phase_breakdown_visualization(stats, output_dir)
        if detailed_phase_path:
            visualization_paths["detailed_phase_breakdown"] = detailed_phase_path
            logger.info(f"Created detailed phase breakdown visualization at {detailed_phase_path}")
    except Exception as e:
        logger.error(f"Error creating detailed phase breakdown visualization: {e}")
    
    # Create time-to-submission visualization
    try:
        time_to_submission_path = create_time_to_submission_visualization(stats, output_dir)
        if time_to_submission_path:
            visualization_paths["time_to_submission"] = time_to_submission_path
            logger.info(f"Created time-to-submission visualization at {time_to_submission_path}")
    except Exception as e:
        logger.error(f"Error creating time-to-submission visualization: {e}")
    
    # Create backend phase comparison visualization
    try:
        backend_phase_comp_path = create_backend_phase_comparison(stats, output_dir)
        if backend_phase_comp_path:
            visualization_paths["backend_phase_comparison"] = backend_phase_comp_path
            logger.info(f"Created backend phase comparison visualization at {backend_phase_comp_path}")
    except Exception as e:
        logger.error(f"Error creating backend phase comparison visualization: {e}")
    
    return visualization_paths


def create_time_to_submission_visualization(stats: Dict[str, Any], output_dir: str) -> str:
    """
    Create a visualization showing time-to-submission metrics.
    
    Args:
        stats: Dictionary of timing statistics
        output_dir: Directory to save visualization
        
    Returns:
        Path to the visualization file
    """
    if "timing_details" not in stats or not stats["timing_details"]:
        return None
    
    try:
        # Collect detailed phase timings if available
        time_to_submission_data = []
        backend_data = []
        
        # Extract from overall stats first
        if "overall" in stats and "detailed_phases" in stats["overall"]:
            detailed_phases = stats["overall"]["detailed_phases"]
            if "time_to_submission" in detailed_phases and "mean" in detailed_phases["time_to_submission"]:
                mean_submission = detailed_phases["time_to_submission"]["mean"]
                time_to_submission_data.append(mean_submission)
        
        # Extract from individual jobs
        if "by_backend_location_size_format" in stats:
            for key, value in stats["by_backend_location_size_format"].items():
                if not isinstance(value, dict) or "error" in value:
                    continue
                
                backend = key.split('_')[0]
                
                if "detailed_phases" in value and "time_to_submission" in value["detailed_phases"]:
                    submission_stats = value["detailed_phases"]["time_to_submission"]
                    if "mean" in submission_stats:
                        time_to_submission_data.append(submission_stats["mean"])
                        backend_data.append((backend, submission_stats["mean"]))
        
        # Skip if not enough data
        if not time_to_submission_data:
            return None
            
        # Create figure with two subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
        
        # Create histogram for time to submission distribution
        ax1.hist(time_to_submission_data, bins=10, color='skyblue', edgecolor='black')
        ax1.set_xlabel('Time to Submission (seconds)', fontsize=12)
        ax1.set_ylabel('Frequency', fontsize=12)
        ax1.set_title('Distribution of Time to Submission', fontsize=14)
        ax1.grid(axis='y', alpha=0.75)
        
        # Create a bar chart comparing backends if we have multiple backends
        if backend_data:
            backend_df = pd.DataFrame(backend_data, columns=['Backend', 'Time to Submission'])
            backend_avg = backend_df.groupby('Backend')['Time to Submission'].mean().reset_index()
            
            # Plot only if we have multiple backends
            if len(backend_avg) > 1:
                bars = ax2.bar(backend_avg['Backend'], backend_avg['Time to Submission'], color='lightgreen')
                ax2.set_xlabel('Backend', fontsize=12)
                ax2.set_ylabel('Time to Submission (seconds)', fontsize=12)
                ax2.set_title('Average Time to Submission by Backend', fontsize=14)
                ax2.grid(axis='y', alpha=0.75)
                
                # Add value labels on bars
                for bar in bars:
                    height = bar.get_height()
                    ax2.annotate(f'{height:.2f}s',
                                xy=(bar.get_x() + bar.get_width() / 2, height),
                                xytext=(0, 3),
                                textcoords="offset points",
                                ha='center', va='bottom')
        
        plt.tight_layout()
        
        # Save figure
        fig_path = os.path.join(output_dir, "time_to_submission.png")
        plt.savefig(fig_path, dpi=300)
        plt.close(fig)
        
        return fig_path
    
    except Exception as e:
        logger.error(f"Error creating time to submission visualization: {e}")
        return None


def create_backend_phase_comparison(stats: Dict[str, Any], output_dir: str) -> str:
    """
    Create a visualization comparing different backends with their phase breakdowns.
    
    Args:
        stats: Dictionary of timing statistics
        output_dir: Directory to save visualization
        
    Returns:
        Path to the visualization file
    """
    if "by_backend_location_size_format" not in stats:
        return None
    
    try:
        # Collect phase data by backend
        backend_phases = defaultdict(lambda: defaultdict(float))
        backend_counts = defaultdict(int)
        
        for key, value in stats["by_backend_location_size_format"].items():
            if not isinstance(value, dict) or "error" in value:
                continue
                
            backend = key.split('_')[0]
            
            # Add phase times if available
            phases = ["submission_time", "queue_time", "processing_time", "download_time"]
            
            for phase in phases:
                if phase in value and "mean" in value[phase]:
                    backend_phases[backend][phase] += value[phase]["mean"]
            
            # Increment count
            backend_counts[backend] += 1
        
        # Calculate averages and prepare data for visualization
        
        # Skip if not enough backend data
        if len(backend_phases) <= 1:
            return None
            
        # Prepare data for plotting
        backends = list(backend_phases.keys())
        df_data = []
        
        for backend in backends:
            count = backend_counts[backend]
            for phase in phases:
                if count > 0:
                    avg_time = backend_phases[backend][phase] / count
                    df_data.append({
                        "Backend": backend,
                        "Phase": phase.replace("_time", "").capitalize(),
                        "Time (seconds)": avg_time
                    })
        
        # Create DataFrame
        df = pd.DataFrame(df_data)
        
        # Create figure
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 16))
        
        # 1. Create stacked bar chart
        pivot_df = df.pivot(index="Backend", columns="Phase", values="Time (seconds)")
        pivot_df.plot(kind="bar", stacked=True, ax=ax1, colormap="viridis")
        
        # Add total time labels on top
        for i, backend in enumerate(pivot_df.index):
            total_time = pivot_df.loc[backend].sum()
            ax1.text(i, total_time + 2, f"{total_time:.1f}s", ha="center", fontsize=12)
        
        ax1.set_title("Backend Comparison - Processing Phases", fontsize=16)
        ax1.set_ylabel("Time (seconds)", fontsize=14)
        ax1.set_xlabel("Backend", fontsize=14)
        ax1.legend(title="Processing Phase", fontsize=12)
        ax1.grid(axis="y", linestyle="--", alpha=0.7)
        
        # 2. Create proportional stacked bar chart (normalized to 100%)
        normalized_df = pivot_df.div(pivot_df.sum(axis=1), axis=0) * 100
        normalized_df.plot(kind="bar", stacked=True, ax=ax2, colormap="viridis")
        
        # Add percentage labels
        for i, backend in enumerate(normalized_df.index):
            ax2.text(i, 103, "100%", ha="center", fontsize=12)
            
            # Add proportional labels on bars
            current_height = 0
            for phase in normalized_df.columns:
                height = normalized_df.loc[backend, phase]
                if height > 5:  # Only show label if segment is large enough
                    ax2.text(i, current_height + height/2, f"{height:.1f}%", 
                            ha="center", va="center", fontsize=10, color="white")
                current_height += height
        
        ax2.set_title("Backend Comparison - Proportional Phase Distribution", fontsize=16)
        ax2.set_ylabel("Percentage of Total Time", fontsize=14)
        ax2.set_xlabel("Backend", fontsize=14)
        ax2.legend(title="Processing Phase", fontsize=12)
        ax2.grid(axis="y", linestyle="--", alpha=0.7)
        
        plt.tight_layout()
        
        # Save figure
        fig_path = os.path.join(output_dir, "backend_phase_comparison.png")
        plt.savefig(fig_path, dpi=300)
        plt.close(fig)
        
        return fig_path
        
    except Exception as e:
        logger.error(f"Error creating backend phase comparison visualization: {e}")
        return None


def export_timing_stats_to_csv(stats: Dict[str, Any], output_dir: str) -> Dict[str, str]:
    """
    Export timing statistics to CSV files for easier analysis and visualization.
    
    Args:
        stats: Dictionary of timing statistics
        output_dir: Directory to save CSV files
        
    Returns:
        Dictionary mapping CSV file types to their file paths
    """
    csv_files = {}
    os.makedirs(output_dir, exist_ok=True)
    
    # Export overall statistics to CSV
    if "overall" in stats and stats["overall"].get("successful_jobs", 0) > 0:
        try:
            # Create overall timing statistics CSV
            overall = stats["overall"]
            phases = [
                ("submission_time", "Job Submission"),
                ("queue_time", "Queue Time"),
                ("processing_time", "Processing"),
                ("download_time", "Download"),
                ("total_time", "Total Time")
            ]
            
            # Prepare data for the CSV
            phase_data = []
            for phase_key, phase_label in phases:
                if phase_key in overall:
                    phase_stats = overall[phase_key]
                    if "error" not in phase_stats:
                        phase_data.append({
                            "Phase": phase_label,
                            "Minimum (s)": phase_stats.get("min", 0),
                            "Maximum (s)": phase_stats.get("max", 0),
                            "Mean (s)": phase_stats.get("mean", 0),
                            "Median (s)": phase_stats.get("median", 0),
                            "Std Dev (s)": phase_stats.get("std_dev", 0),
                            "Count": phase_stats.get("count", 0)
                        })
            
            if phase_data:
                # Create DataFrame and save to CSV
                df = pd.DataFrame(phase_data)
                csv_path = os.path.join(output_dir, "overall_timing_stats.csv")
                df.to_csv(csv_path, index=False)
                csv_files["overall_timing"] = csv_path
                logger.info(f"Exported overall timing statistics to {csv_path}")
                
            # Export detailed phase timing statistics if available
            if "detailed_phases" in overall:
                detailed_phases = overall["detailed_phases"]
                detailed_data = []
                
                phase_keys = [
                    ("time_to_submission", "Time to Submission"),
                    ("time_to_queue", "Time to Queue"),
                    ("time_to_processing", "Time to Processing Start"),
                    ("time_to_finish", "Time to Processing Finish"),
                    ("time_to_download", "Time to Download Results")
                ]
                
                for phase_key, phase_label in phase_keys:
                    if phase_key in detailed_phases:
                        phase_stats = detailed_phases[phase_key]
                        if "error" not in phase_stats:
                            detailed_data.append({
                                "Phase": phase_label,
                                "Minimum (s)": phase_stats.get("min", 0),
                                "Maximum (s)": phase_stats.get("max", 0),
                                "Mean (s)": phase_stats.get("mean", 0),
                                "Median (s)": phase_stats.get("median", 0),
                                "Std Dev (s)": phase_stats.get("std_dev", 0),
                                "Count": phase_stats.get("count", 0)
                            })
                
                if detailed_data:
                    # Create DataFrame and save to CSV
                    df = pd.DataFrame(detailed_data)
                    csv_path = os.path.join(output_dir, "detailed_phase_stats.csv")
                    df.to_csv(csv_path, index=False)
                    csv_files["detailed_phases"] = csv_path
                    logger.info(f"Exported detailed phase statistics to {csv_path}")
        except Exception as e:
            logger.error(f"Error exporting overall statistics to CSV: {e}")
    
    # Export backend comparison statistics
    if "by_backend_location_size_format" in stats:
        try:
            # Group by backend
            backend_data = []
            
            for key, value in stats["by_backend_location_size_format"].items():
                if not isinstance(value, dict) or "error" in value:
                    continue
                    
                parts = key.split('_')
                if len(parts) >= 4:
                    backend, location, bbox_size, file_format = parts[:4]
                    
                    # Extract timing statistics
                    row = {
                        "Backend": backend,
                        "Location": location,
                        "Bbox Size": bbox_size,
                        "Format": file_format,
                        "Success Rate (%)": value.get("success_rate", 0) * 100
                    }
                    
                    # Add timing metrics
                    metrics = ["submission_time", "queue_time", "processing_time", "download_time", "total_time"]
                    for metric in metrics:
                        if metric in value and "mean" in value[metric]:
                            row[f"{metric.replace('_time', '').capitalize()} Mean (s)"] = value[metric]["mean"]
                            row[f"{metric.replace('_time', '').capitalize()} Median (s)"] = value[metric]["median"]
                    
                    backend_data.append(row)
            
            if backend_data:
                # Create DataFrame and save to CSV
                df = pd.DataFrame(backend_data)
                csv_path = os.path.join(output_dir, "backend_comparison.csv")
                df.to_csv(csv_path, index=False)
                csv_files["backend_comparison"] = csv_path
                logger.info(f"Exported backend comparison statistics to {csv_path}")
                
                # Create a simpler pivot table focused on processing times
                # Pivot by backend and metric, summarizing mean values
                pivot_data = []
                for row in backend_data:
                    backend = row["Backend"]
                    for metric in ["Submission", "Queue", "Processing", "Download", "Total"]:
                        col_name = f"{metric} Mean (s)"
                        if col_name in row:
                            pivot_data.append({
                                "Backend": backend,
                                "Phase": metric,
                                "Time (seconds)": row[col_name]
                            })
                
                if pivot_data:
                    pivot_df = pd.DataFrame(pivot_data)
                    # Wide format (backends as columns)
                    wide_pivot = pivot_df.pivot(index="Phase", columns="Backend", values="Time (seconds)")
                    csv_path = os.path.join(output_dir, "processing_times_by_backend.csv")
                    wide_pivot.to_csv(csv_path)
                    csv_files["processing_times"] = csv_path
                    logger.info(f"Exported processing times by backend to {csv_path}")
        
        except Exception as e:
            logger.error(f"Error exporting backend comparison to CSV: {e}")
    
    return csv_files


def generate_timing_report(stats: Dict[str, Any], visualizations: Dict[str, str], output_dir: str) -> str:
    """
    Generate a Markdown report of timing statistics.
    
    Args:
        stats: Dictionary of timing statistics
        visualizations: Dictionary of visualization paths
        output_dir: Directory to save the report
        
    Returns:
        Path to the generated report file
    """
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "timing_analysis_report.md")
    
    try:
        with open(report_path, 'w') as file:
            # Report header
            file.write("# OpenEO Backend Timing Analysis Report\n\n")
            file.write(f"*Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n")
            
            # Add filters section if any were applied
            if stats.get("filters"):
                file.write("## Analysis Filters\n\n")
                
                for key, value in stats["filters"].items():
                    if value:
                        file.write(f"- **{key.capitalize()}**: {value}\n")
                file.write("\n")
            
            # Overall statistics
            file.write("## Overall Performance\n\n")
            
            overall = stats.get("overall", {})
            if "error" in overall:
                file.write(f"*{overall['error']}*\n\n")
            else:
                file.write("### Job Success Rate\n\n")
                file.write(f"- **Total Jobs**: {overall.get('total_jobs', 0)}\n")
                file.write(f"- **Successful**: {overall.get('successful_jobs', 0)} ")
                file.write(f"({overall.get('success_rate', 0) * 100:.1f}%)\n")
                file.write(f"- **Failed**: {overall.get('failed_jobs', 0)}\n\n")
                
                # Add timing visualizations if available
                if "processing_phases" in visualizations:
                    vis_path = os.path.relpath(visualizations["processing_phases"], output_dir)
                    file.write(f"![Processing Phases Timing]({vis_path})\n\n")
                
                # Add phase proportion visualization if available
                if "phase_proportions" in visualizations:
                    vis_path = os.path.relpath(visualizations["phase_proportions"], output_dir)
                    file.write("### Time Spent in Each Processing Phase\n\n")
                    file.write(f"![Time Spent in Each Processing Phase]({vis_path})\n\n")
                
                # Add detailed phase breakdown visualization if available
                if "detailed_phase_breakdown" in visualizations:
                    vis_path = os.path.relpath(visualizations["detailed_phase_breakdown"], output_dir)
                    file.write("### Detailed Phase Breakdown\n\n")
                    file.write("This visualization shows both the proportional breakdown of processing phases and the absolute time spent in each phase.\n\n")
                    file.write(f"![Detailed Phase Breakdown]({vis_path})\n\n")
                
                # Add time to submission visualization if available
                if "time_to_submission" in visualizations:
                    vis_path = os.path.relpath(visualizations["time_to_submission"], output_dir)
                    file.write("### Time to Submission Analysis\n\n")
                    file.write("This visualization shows the distribution of time-to-submission values and compares submission times across backends.\n\n")
                    file.write(f"![Time to Submission Analysis]({vis_path})\n\n")
                
                # Add status transition visualization if available
                if "status_timing" in visualizations:
                    vis_path = os.path.relpath(visualizations["status_timing"], output_dir)
                    file.write("### Job Status Transition Times\n\n")
                    file.write(f"![Job Status Transition Times]({vis_path})\n\n")
                
                file.write("### Processing Phase Statistics (seconds)\n\n")
                
                # Table header
                file.write("| Phase | Minimum | Maximum | Mean | Median | Std Dev |\n")
                file.write("|-------|---------|---------|------|--------|--------|\n")
                
                # Add rows for each timing metric
                metrics = [
                    ("submission_time", "Job Submission"),
                    ("queue_time", "Queue Time"), 
                    ("processing_time", "Processing"),
                    ("download_time", "Download"),
                    ("total_time", "Total Time")
                ]
                
                # Add detailed phase information if available
                if "detailed_phases" in overall:
                    file.write("\n### Detailed Phase Timing Statistics (seconds)\n\n")
                    file.write("| Phase | Minimum | Maximum | Mean | Median | Std Dev |\n")
                    file.write("|-------|---------|---------|------|--------|--------|\n")
                    
                    detailed_phases = [
                        ("time_to_submission", "Time to Submission"),
                        ("time_to_queue", "Time to Queue"),
                        ("time_to_processing", "Time to Processing Start"),
                        ("time_to_finish", "Time to Processing Finish"),
                        ("time_to_download", "Time to Download Results")
                    ]
                    
                    for phase_key, phase_label in detailed_phases:
                        if phase_key in overall["detailed_phases"]:
                            phase_stats = overall["detailed_phases"][phase_key]
                            
                            if "error" in phase_stats:
                                min_val = max_val = mean_val = median_val = std_val = "N/A"
                            else:
                                min_val = f"{phase_stats.get('min', 0):.2f}"
                                max_val = f"{phase_stats.get('max', 0):.2f}"
                                mean_val = f"{phase_stats.get('mean', 0):.2f}"
                                median_val = f"{phase_stats.get('median', 0):.2f}"
                                std_val = f"{phase_stats.get('std_dev', 0):.2f}"
                            
                            file.write(f"| {phase_label} | {min_val} | {max_val} | {mean_val} | {median_val} | {std_val} |\n")
                
                for metric_key, metric_label in metrics:
                    metric_stats = overall.get(metric_key, {})
                    
                    if "error" in metric_stats:
                        min_val = max_val = mean_val = median_val = std_val = "N/A"
                    else:
                        min_val = f"{metric_stats.get('min', 0):.2f}"
                        max_val = f"{metric_stats.get('max', 0):.2f}"
                        mean_val = f"{metric_stats.get('mean', 0):.2f}"
                        median_val = f"{metric_stats.get('median', 0):.2f}"
                        std_val = f"{metric_stats.get('std_dev', 0):.2f}"
                    
                    file.write(f"| {metric_label} | {min_val} | {max_val} | {mean_val} | {median_val} | {std_val} |\n")
                
                # Download statistics
                file.write("\n### Download Statistics\n\n")
                download_stats = overall.get("download_stats", {})
                
                if download_stats:
                    file.write(f"- **Total Files Downloaded**: {download_stats.get('total_files', 0)}\n")
                    file.write(f"- **Average Files per Job**: {download_stats.get('avg_files_per_job', 0):.2f}\n")
                    file.write(f"- **Total Download Size**: {download_stats.get('total_size_mb', 0):.2f} MB\n")
                    file.write(f"- **Average Size per Job**: {download_stats.get('avg_size_per_job_mb', 0):.2f} MB\n\n")
                else:
                    file.write("*No download statistics available*\n\n")
            
            # Backend comparison
            if "backend_comparison" in visualizations:
                file.write("## Backend Performance Comparison\n\n")
                vis_path = os.path.relpath(visualizations["backend_comparison"], output_dir)
                file.write(f"![Backend Performance Comparison]({vis_path})\n\n")
                
                # Add backend phase comparison if available
                if "backend_phase_comparison" in visualizations:
                    vis_path = os.path.relpath(visualizations["backend_phase_comparison"], output_dir)
                    file.write("### Backend Phase Comparison\n\n")
                    file.write("This visualization shows how different backends spend time in each processing phase, both in absolute terms and as percentages of total time.\n\n")
                    file.write(f"![Backend Phase Comparison]({vis_path})\n\n")
            
            # Detailed statistics by backend/location/size/format
            file.write("## Detailed Statistics\n\n")
            
            for key, group_stats in stats.get("by_backend_location_size_format", {}).items():
                parts = key.split('_')
                if len(parts) >= 4:
                    backend, location, bbox_size, file_format = parts[:4]
                    
                    file.write(f"### {backend} - {location} ({bbox_size}) - {file_format} Format\n\n")
                    
                    if "error" in group_stats:
                        file.write(f"*{group_stats['error']}*\n\n")
                        continue
                    
                    file.write(f"- **Total Jobs**: {group_stats.get('total_jobs', 0)}\n")
                    file.write(f"- **Success Rate**: {group_stats.get('success_rate', 0) * 100:.1f}%\n\n")
                    
                    file.write("#### Processing Times (seconds)\n\n")
                    file.write("| Phase | Mean | Median | Min | Max | Std Dev |\n")
                    file.write("|-------|------|--------|-----|-----|--------|\n")
                    
                    metrics = [
                        ("submission_time", "Job Submission"),
                        ("queue_time", "Queue Time"),
                        ("processing_time", "Processing"),
                        ("download_time", "Download"),
                        ("total_time", "Total Time")
                    ]
                    
                    for metric_key, metric_label in metrics:
                        metric_stats = group_stats.get(metric_key, {})
                        
                        if "error" in metric_stats:
                            mean = median = min_val = max_val = std = "N/A"
                        else:
                            mean = f"{metric_stats.get('mean', 0):.2f}"
                            median = f"{metric_stats.get('median', 0):.2f}"
                            min_val = f"{metric_stats.get('min', 0):.2f}"
                            max_val = f"{metric_stats.get('max', 0):.2f}"
                            std = f"{metric_stats.get('std_dev', 0):.2f}"
                        
                        file.write(f"| {metric_label} | {mean} | {median} | {min_val} | {max_val} | {std} |\n")
                    
                    file.write("\n")
            
            # Add timestamp
            file.write("\n---\n")
            file.write(f"*Analysis performed on data from: {stats.get('timestamp', 'unknown time')}*\n")
        
        logger.info(f"Timing analysis report generated at {report_path}")
        return report_path
    
    except Exception as e:
        logger.error(f"Error generating timing report: {e}")
        return None


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Analyze OpenEO Backend Timing Statistics")
    
    parser.add_argument("--input", default="output",
                       help="Directory containing OpenEO test results (default: 'output')")
    
    parser.add_argument("--output", default=None,
                       help="Directory to save output files (default: <input>/timing_analysis)")
    
    parser.add_argument("--backend", default=None,
                       help="Filter by specific backend name")
    
    parser.add_argument("--format", default=None,
                       help="Filter by specific format (GTiff or NetCDF)")
    
    parser.add_argument("--recursive", action="store_true",
                       help="Search recursively in all subdirectories")
    
    args = parser.parse_args()
    
    # Set output directory
    output_dir = args.output or os.path.join(args.input, "timing_analysis")
    os.makedirs(output_dir, exist_ok=True)
    
    # Create filters dictionary
    filters = {
        "backend_name": args.backend,
        "format": args.format,
    }
    
    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None}
    
    logger.info(f"Starting timing analysis for {args.input}")
    if filters:
        logger.info(f"Applying filters: {filters}")
    
    # Analyze logs
    stats = analyze_logs(args.input, filters)
    
    if not stats or stats.get("error"):
        logger.error(f"Analysis failed: {stats.get('error', 'No valid results found')}")
        return 1
    
    # Create visualizations directory if matplotlib is available
    if MATPLOTLIB_AVAILABLE:
        vis_dir = os.path.join(output_dir, "visualizations")
        os.makedirs(vis_dir, exist_ok=True)
        
        # Create visualizations
        visualizations = create_timing_visualizations(stats, vis_dir)
        logger.info(f"Created {len(visualizations)} visualizations in {vis_dir}")
        
        # Generate report
        report_path = generate_timing_report(stats, visualizations, output_dir)
        if report_path:
            logger.info(f"Report generated: {report_path}")
    else:
        logger.warning("Matplotlib not available, skipping visualizations")
        visualizations = {}
        report_path = None
    
    # Save raw statistics as JSON
    stats_path = os.path.join(output_dir, "timing_statistics.json")
    with open(stats_path, 'w') as file:
        json.dump(stats, file, indent=2, default=str)
    logger.info(f"Raw statistics saved to: {stats_path}")
    
    # Export timing statistics to CSV if pandas is available
    if PANDAS_AVAILABLE:
        csv_files = export_timing_stats_to_csv(stats, output_dir)
        if csv_files:
            logger.info(f"CSV statistics exported to: {', '.join(csv_files.values())}")
    else:
        logger.warning("Pandas not available, skipping CSV export")
    
    logger.info(f"Analysis complete. Results saved to {output_dir}")
    
    return 0


if __name__ == "__main__":
    main()
