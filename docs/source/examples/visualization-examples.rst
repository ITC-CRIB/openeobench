Visualization Examples
====================

This section demonstrates various visualization capabilities of openEObench for analyzing and presenting results.

Basic Result Visualization
---------------------------

Visualize results from multiple scenario runs:

.. code-block:: bash

   # Create visualizations for comparison
   openeobench visualize results/backend1/ results/backend2/ --output comparison.md --format both

This generates:
- A markdown report with embedded images and statistics
- A PNG matrix showing all results side-by-side
- Individual PNG files for each GeoTIFF

Matrix Visualization
--------------------

Create comprehensive matrix visualizations for multiple results:

.. code-block:: bash

   # Visualize results from different scenarios and backends
   openeobench visualize \\
     scenarios/ndvi/cdse/ \\
     scenarios/ndvi/vito/ \\
     scenarios/ndvi/gee/ \\
     scenarios/evi/cdse/ \\
     scenarios/evi/vito/ \\
     scenarios/evi/gee/ \\
     --output ndvi_evi_matrix.png --format png

Advanced Visualization Workflows
---------------------------------

Combine multiple visualization steps for comprehensive analysis:

.. code-block:: bash
   :caption: comprehensive_visualization.sh

   #!/bin/bash
   
   # Setup directories
   mkdir -p visualizations/{individual,matrices,reports}
   
   # 1. Individual scenario visualizations
   scenarios=("ndvi" "evi" "savi" "brightness_temperature")
   backends=("cdse" "vito" "gee")
   
   for scenario in "${scenarios[@]}"; do
       for backend in "${backends[@]}"; do
           if [ -d "results/${scenario}/${backend}" ]; then
               echo "Visualizing ${scenario} on ${backend}..."
               openeobench visualize \\
                 "results/${scenario}/${backend}/" \\
                 --output "visualizations/individual/${scenario}_${backend}.md" \\
                 --format both
           fi
       done
   done
   
   # 2. Cross-backend comparison for each scenario
   for scenario in "${scenarios[@]}"; do
       echo "Creating cross-backend comparison for ${scenario}..."
       backend_dirs=""
       for backend in "${backends[@]}"; do
           if [ -d "results/${scenario}/${backend}" ]; then
               backend_dirs="$backend_dirs results/${scenario}/${backend}/"
           fi
       done
       
       if [ -n "$backend_dirs" ]; then
           openeobench visualize $backend_dirs \\
             --output "visualizations/matrices/${scenario}_backends.md" \\
             --format both
       fi
   done
   
   # 3. Cross-scenario comparison for each backend
   for backend in "${backends[@]}"; do
       echo "Creating cross-scenario comparison for ${backend}..."
       scenario_dirs=""
       for scenario in "${scenarios[@]}"; do
           if [ -d "results/${scenario}/${backend}" ]; then
               scenario_dirs="$scenario_dirs results/${scenario}/${backend}/"
           fi
       done
       
       if [ -n "$scenario_dirs" ]; then
           openeobench visualize $scenario_dirs \\
             --output "visualizations/matrices/${backend}_scenarios.md" \\
             --format both
       fi
   done
   
   # 4. Complete matrix (all scenarios × all backends)
   echo "Creating complete result matrix..."
   all_dirs=""
   for scenario in "${scenarios[@]}"; do
       for backend in "${backends[@]}"; do
           if [ -d "results/${scenario}/${backend}" ]; then
               all_dirs="$all_dirs results/${scenario}/${backend}/"
           fi
       done
   done
   
   if [ -n "$all_dirs" ]; then
       openeobench visualize $all_dirs \\
         --output "visualizations/complete_matrix.md" \\
         --format both
   fi
   
   echo "Visualization workflow complete!"

Custom Analysis Scripts
-----------------------

Create custom scripts for specialized visualization needs:

.. code-block:: python
   :caption: custom_analysis.py

   #!/usr/bin/env python3
   import os
   import matplotlib.pyplot as plt
   import numpy as np
   from osgeo import gdal
   from pathlib import Path
   import seaborn as sns
   
   def analyze_result_statistics():
       """Create custom statistical analysis of results."""
       
       # Find all GeoTIFF files
       result_files = list(Path("results").glob("**/*.tif"))
       
       statistics = []
       
       for tiff_file in result_files:
           # Parse path for metadata
           parts = tiff_file.parts
           scenario = parts[1] if len(parts) > 1 else "unknown"
           backend = parts[2] if len(parts) > 2 else "unknown"
           
           # Read raster data
           dataset = gdal.Open(str(tiff_file))
           if dataset:
               band = dataset.GetRasterBand(1)
               data = band.ReadAsArray()
               
               # Calculate statistics
               stats = {
                   'scenario': scenario,
                   'backend': backend,
                   'file': tiff_file.name,
                   'mean': np.nanmean(data),
                   'std': np.nanstd(data),
                   'min': np.nanmin(data),
                   'max': np.nanmax(data),
                   'valid_pixels': np.sum(~np.isnan(data)),
                   'total_pixels': data.size
               }
               
               statistics.append(stats)
               dataset = None
       
       return statistics
   
   def create_comparison_plots(statistics):
       """Create comparison plots from statistics."""
       
       # Convert to DataFrame-like structure
       scenarios = list(set(s['scenario'] for s in statistics))
       backends = list(set(s['backend'] for s in statistics))
       
       # Create subplots
       fig, axes = plt.subplots(2, 2, figsize=(15, 12))
       fig.suptitle('OpenEO Backend Comparison Analysis', fontsize=16)
       
       # Plot 1: Mean values by scenario and backend
       mean_data = {}
       for scenario in scenarios:
           mean_data[scenario] = {}
           for backend in backends:
               values = [s['mean'] for s in statistics 
                        if s['scenario'] == scenario and s['backend'] == backend]
               mean_data[scenario][backend] = np.mean(values) if values else np.nan
       
       # Convert to matrix for heatmap
       matrix = []
       for scenario in scenarios:
           row = [mean_data[scenario].get(backend, np.nan) for backend in backends]
           matrix.append(row)
       
       sns.heatmap(matrix, 
                   xticklabels=backends, 
                   yticklabels=scenarios, 
                   annot=True, 
                   fmt='.3f',
                   ax=axes[0,0])
       axes[0,0].set_title('Mean Values by Scenario and Backend')
       
       # Plot 2: Standard deviation comparison
       std_values = [s['std'] for s in statistics]
       scenario_labels = [s['scenario'] for s in statistics]
       backend_labels = [s['backend'] for s in statistics]
       
       axes[0,1].scatter(range(len(std_values)), std_values, 
                        c=[hash(label) for label in scenario_labels])
       axes[0,1].set_title('Standard Deviation Distribution')
       axes[0,1].set_ylabel('Standard Deviation')
       
       # Plot 3: Valid pixel percentage
       valid_percentages = [s['valid_pixels']/s['total_pixels']*100 for s in statistics]
       
       backend_positions = {backend: i for i, backend in enumerate(backends)}
       x_positions = [backend_positions[s['backend']] for s in statistics]
       
       axes[1,0].scatter(x_positions, valid_percentages)
       axes[1,0].set_xticks(range(len(backends)))
       axes[1,0].set_xticklabels(backends, rotation=45)
       axes[1,0].set_title('Valid Pixel Percentage by Backend')
       axes[1,0].set_ylabel('Valid Pixels (%)')
       
       # Plot 4: Range (max - min) comparison
       ranges = [s['max'] - s['min'] for s in statistics]
       scenario_positions = {scenario: i for i, scenario in enumerate(scenarios)}
       x_positions = [scenario_positions[s['scenario']] for s in statistics]
       
       axes[1,1].scatter(x_positions, ranges)
       axes[1,1].set_xticks(range(len(scenarios)))
       axes[1,1].set_xticklabels(scenarios, rotation=45)
       axes[1,1].set_title('Value Range by Scenario')
       axes[1,1].set_ylabel('Range (max - min)')
       
       plt.tight_layout()
       plt.savefig('visualizations/statistical_analysis.png', dpi=300, bbox_inches='tight')
       plt.close()
   
   def generate_analysis_report(statistics):
       """Generate comprehensive analysis report."""
       
       with open('visualizations/analysis_report.md', 'w') as f:
           f.write("# OpenEO Backend Analysis Report\\n\\n")
           f.write(f"Generated: {os.popen('date').read().strip()}\\n\\n")
           
           # Summary statistics
           f.write("## Summary Statistics\\n\\n")
           f.write(f"- Total files analyzed: {len(statistics)}\\n")
           f.write(f"- Scenarios: {len(set(s['scenario'] for s in statistics))}\\n")
           f.write(f"- Backends: {len(set(s['backend'] for s in statistics))}\\n\\n")
           
           # Per-backend summary
           f.write("## Backend Performance Summary\\n\\n")
           backends = set(s['backend'] for s in statistics)
           
           for backend in sorted(backends):
               backend_stats = [s for s in statistics if s['backend'] == backend]
               f.write(f"### {backend}\\n\\n")
               f.write(f"- Files processed: {len(backend_stats)}\\n")
               
               if backend_stats:
                   avg_mean = np.mean([s['mean'] for s in backend_stats])
                   avg_std = np.mean([s['std'] for s in backend_stats])
                   avg_valid = np.mean([s['valid_pixels']/s['total_pixels']*100 for s in backend_stats])
                   
                   f.write(f"- Average mean value: {avg_mean:.3f}\\n")
                   f.write(f"- Average std deviation: {avg_std:.3f}\\n")
                   f.write(f"- Average valid pixels: {avg_valid:.1f}%\\n")
               
               f.write("\\n")
           
           # Include statistical plot
           f.write("## Statistical Analysis\\n\\n")
           f.write("![Statistical Analysis](statistical_analysis.png)\\n\\n")
           
           # Detailed results table
           f.write("## Detailed Results\\n\\n")
           f.write("| Scenario | Backend | File | Mean | Std | Min | Max | Valid % |\\n")
           f.write("|----------|---------|------|------|-----|-----|-----|---------|\\n")
           
           for s in sorted(statistics, key=lambda x: (x['scenario'], x['backend'])):
               valid_pct = s['valid_pixels']/s['total_pixels']*100
               f.write(f"| {s['scenario']} | {s['backend']} | {s['file']} | ")
               f.write(f"{s['mean']:.3f} | {s['std']:.3f} | {s['min']:.3f} | ")
               f.write(f"{s['max']:.3f} | {valid_pct:.1f}% |\\n")
   
   if __name__ == "__main__":
       print("Analyzing result statistics...")
       stats = analyze_result_statistics()
       
       if stats:
           print(f"Found {len(stats)} result files")
           print("Creating comparison plots...")
           create_comparison_plots(stats)
           
           print("Generating analysis report...")
           generate_analysis_report(stats)
           
           print("Custom analysis complete!")
           print("Results saved in visualizations/")
       else:
           print("No result files found for analysis")

Time Series Visualization
-------------------------

Create time series visualizations for temporal analysis:

.. code-block:: python
   :caption: time_series_viz.py

   #!/usr/bin/env python3
   import matplotlib.pyplot as plt
   import pandas as pd
   from pathlib import Path
   import json
   from datetime import datetime
   
   def create_service_performance_timeline():
       """Create timeline visualization of service performance."""
       
       # Collect service check data
       service_files = list(Path("outputs").glob("*.csv"))
       
       if not service_files:
           print("No service data files found")
           return
       
       all_data = []
       
       for file in service_files:
           try:
               df = pd.read_csv(file)
               df['date'] = file.stem
               all_data.append(df)
           except Exception as e:
               print(f"Error reading {file}: {e}")
       
       if not all_data:
           return
       
       # Combine all data
       combined_df = pd.concat(all_data, ignore_index=True)
       combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
       
       # Create timeline plot
       fig, axes = plt.subplots(2, 1, figsize=(15, 10))
       
       # Response time timeline
       for url in combined_df['url'].unique():
           url_data = combined_df[combined_df['url'] == url]
           axes[0].plot(url_data['timestamp'], url_data['response_time'], 
                       label=url.split('//')[1].split('.')[0], marker='o', markersize=3)
       
       axes[0].set_title('Service Response Times Over Time')
       axes[0].set_ylabel('Response Time (seconds)')
       axes[0].legend()
       axes[0].grid(True)
       
       # Success rate timeline
       combined_df['success'] = (combined_df['status_code'] == 200).astype(int)
       
       for url in combined_df['url'].unique():
           url_data = combined_df[combined_df['url'] == url]
           # Calculate daily success rate
           daily_success = url_data.groupby(url_data['timestamp'].dt.date)['success'].mean()
           axes[1].plot(daily_success.index, daily_success.values * 100, 
                       label=url.split('//')[1].split('.')[0], marker='s', markersize=4)
       
       axes[1].set_title('Service Availability Over Time')
       axes[1].set_ylabel('Availability (%)')
       axes[1].set_ylim(0, 105)
       axes[1].legend()
       axes[1].grid(True)
       
       plt.tight_layout()
       plt.savefig('visualizations/service_timeline.png', dpi=300, bbox_inches='tight')
       plt.close()
   
   def create_execution_time_trends():
       """Create visualization of execution time trends."""
       
       # Find all result files
       result_files = list(Path("results").glob("**/results.json"))
       
       execution_data = []
       
       for file in result_files:
           try:
               with open(file, 'r') as f:
                   data = json.load(f)
               
               # Extract timing information
               parts = file.parts
               scenario = parts[1] if len(parts) > 1 else "unknown"
               backend = parts[2] if len(parts) > 2 else "unknown"
               
               execution_data.append({
                   'scenario': scenario,
                   'backend': backend,
                   'timestamp': data.get('timestamp', ''),
                   'total_time': data.get('time_total', 0),
                   'execution_time': data.get('time_job_execution', 0),
                   'download_time': data.get('time_download', 0)
               })
               
           except Exception as e:
               print(f"Error reading {file}: {e}")
       
       if not execution_data:
           return
       
       df = pd.DataFrame(execution_data)
       df['timestamp'] = pd.to_datetime(df['timestamp'])
       
       # Create execution time plot
       fig, ax = plt.subplots(figsize=(12, 8))
       
       for backend in df['backend'].unique():
           backend_data = df[df['backend'] == backend]
           ax.scatter(backend_data['timestamp'], backend_data['total_time'], 
                     label=backend, alpha=0.7)
       
       ax.set_title('Execution Time Trends by Backend')
       ax.set_ylabel('Total Time (seconds)')
       ax.legend()
       ax.grid(True)
       
       plt.xticks(rotation=45)
       plt.tight_layout()
       plt.savefig('visualizations/execution_trends.png', dpi=300, bbox_inches='tight')
       plt.close()
   
   if __name__ == "__main__":
       print("Creating time series visualizations...")
       
       create_service_performance_timeline()
       print("Service timeline created")
       
       create_execution_time_trends()
       print("Execution trends created")
       
       print("Time series visualizations complete!")

Interactive Dashboard
---------------------

Create an interactive HTML dashboard:

.. code-block:: python
   :caption: interactive_dashboard.py

   #!/usr/bin/env python3
   import json
   from pathlib import Path
   
   def create_interactive_dashboard():
       """Create interactive HTML dashboard with JavaScript charts."""
       
       # Collect latest data
       latest_summary = find_latest_summary()
       service_data = collect_service_data()
       
       html_content = f"""
   <!DOCTYPE html>
   <html>
   <head>
       <title>openEObench Interactive Dashboard</title>
       <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
       <style>
           body {{ font-family: Arial, sans-serif; margin: 20px; }}
           .dashboard-section {{ margin: 30px 0; }}
           .chart-container {{ width: 100%; height: 400px; margin: 20px 0; }}
           .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }}
           .metric-card {{ 
               background: #f8f9fa; 
               padding: 20px; 
               border-radius: 8px; 
               border-left: 4px solid #007bff; 
           }}
       </style>
   </head>
   <body>
       <h1>openEObench Dashboard</h1>
       <p>Last updated: <span id="lastUpdate"></span></p>
       
       <div class="dashboard-section">
           <h2>Service Status</h2>
           <div class="metrics-grid" id="serviceMetrics"></div>
       </div>
       
       <div class="dashboard-section">
           <h2>Process Compliance</h2>
           <div class="chart-container">
               <canvas id="complianceChart"></canvas>
           </div>
       </div>
       
       <div class="dashboard-section">
           <h2>Response Time Trends</h2>
           <div class="chart-container">
               <canvas id="responseTimeChart"></canvas>
           </div>
       </div>
       
       <script>
           // Update timestamp
           document.getElementById('lastUpdate').textContent = new Date().toLocaleString();
           
           // Sample data (replace with actual data)
           const complianceData = {latest_summary};
           const serviceData = {service_data};
           
           // Create compliance chart
           const complianceCtx = document.getElementById('complianceChart').getContext('2d');
           new Chart(complianceCtx, {{
               type: 'bar',
               data: {{
                   labels: Object.keys(complianceData),
                   datasets: [{{
                       label: 'L1 Compliance',
                       data: Object.values(complianceData).map(d => d.l1_compliance || 0),
                       backgroundColor: 'rgba(75, 192, 192, 0.6)'
                   }}, {{
                       label: 'L2 Compliance', 
                       data: Object.values(complianceData).map(d => d.l2_compliance || 0),
                       backgroundColor: 'rgba(54, 162, 235, 0.6)'
                   }}]
               }},
               options: {{
                   responsive: true,
                   scales: {{
                       y: {{
                           beginAtZero: true,
                           max: 100
                       }}
                   }}
               }}
           }});
           
           // Create response time chart
           const responseCtx = document.getElementById('responseTimeChart').getContext('2d');
           new Chart(responseCtx, {{
               type: 'line',
               data: serviceData,
               options: {{
                   responsive: true,
                   scales: {{
                       y: {{
                           beginAtZero: true
                       }}
                   }}
               }}
           }});
           
           // Add service metrics
           const metricsContainer = document.getElementById('serviceMetrics');
           Object.keys(complianceData).forEach(backend => {{
               const card = document.createElement('div');
               card.className = 'metric-card';
               card.innerHTML = `
                   <h3>${{backend}}</h3>
                   <p>L1: ${{complianceData[backend].l1_compliance || 0}}%</p>
                   <p>L2: ${{complianceData[backend].l2_compliance || 0}}%</p>
                   <p>Status: <span style="color: green;">✓ Online</span></p>
               `;
               metricsContainer.appendChild(card);
           }});
       </script>
   </body>
   </html>
       """
       
       with open('visualizations/interactive_dashboard.html', 'w') as f:
           f.write(html_content)
   
   def find_latest_summary():
       """Find and parse latest compliance summary."""
       # Simplified - return sample data
       return {
           "CDSE": {"l1_compliance": 95, "l2_compliance": 87},
           "VITO": {"l1_compliance": 92, "l2_compliance": 78},
           "GEE": {"l1_compliance": 88, "l2_compliance": 71}
       }
   
   def collect_service_data():
       """Collect service response time data."""
       # Simplified - return sample data structure
       return {
           "labels": ["Day 1", "Day 2", "Day 3", "Day 4", "Day 5"],
           "datasets": [{
               "label": "CDSE",
               "data": [1.2, 1.4, 1.1, 1.3, 1.2],
               "borderColor": "rgb(75, 192, 192)",
               "fill": False
           }, {
               "label": "VITO", 
               "data": [0.8, 0.9, 0.7, 0.8, 0.9],
               "borderColor": "rgb(54, 162, 235)",
               "fill": False
           }]
       }
   
   if __name__ == "__main__":
       create_interactive_dashboard()
       print("Interactive dashboard created: visualizations/interactive_dashboard.html")

Complete Visualization Pipeline
-------------------------------

Combine all visualization tools into a single pipeline:

.. code-block:: bash
   :caption: complete_visualization_pipeline.sh

   #!/bin/bash
   
   echo "Starting complete visualization pipeline..."
   
   # Setup output directory
   mkdir -p visualizations/{matrices,reports,custom,interactive}
   
   # 1. Standard openEObench visualizations
   echo "Step 1: Creating standard visualizations..."
   openeobench visualize results/*/ --output visualizations/complete_results.md --format both
   
   # 2. Custom statistical analysis
   echo "Step 2: Running custom statistical analysis..."
   python3 custom_analysis.py
   
   # 3. Time series analysis
   echo "Step 3: Creating time series visualizations..."
   python3 time_series_viz.py
   
   # 4. Interactive dashboard
   echo "Step 4: Building interactive dashboard..."
   python3 interactive_dashboard.py
   
   # 5. Generate comprehensive report
   echo "Step 5: Generating comprehensive report..."
   cat > visualizations/comprehensive_report.md << EOF
   # openEObench Comprehensive Visualization Report
   
   Generated: $(date)
   
   ## Standard Results Visualization
   
   ![Complete Results Matrix](complete_results.png)
   
   ## Statistical Analysis
   
   ![Statistical Analysis](custom/statistical_analysis.png)
   
   ## Time Series Analysis
   
   ### Service Performance Timeline
   ![Service Timeline](service_timeline.png)
   
   ### Execution Time Trends  
   ![Execution Trends](execution_trends.png)
   
   ## Interactive Dashboard
   
   [View Interactive Dashboard](interactive/interactive_dashboard.html)
   
   ## Summary
   
   This comprehensive visualization analysis provides multiple perspectives on OpenEO backend performance:
   
   1. **Matrix visualizations** show spatial results side-by-side
   2. **Statistical analysis** reveals numerical differences between backends
   3. **Time series plots** track performance trends over time
   4. **Interactive dashboard** enables real-time monitoring
   
   EOF
   
   echo "Visualization pipeline complete!"
   echo "Main report: visualizations/comprehensive_report.md"
   echo "Interactive dashboard: visualizations/interactive/interactive_dashboard.html"

This comprehensive visualization suite provides:

1. **Standard matrix visualizations** for spatial result comparison
2. **Custom statistical analysis** with detailed metrics
3. **Time series visualizations** for trend analysis
4. **Interactive dashboards** for real-time monitoring
5. **Automated pipeline** for regular report generation
