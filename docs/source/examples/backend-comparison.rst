Backend Comparison
==================

This example shows how to systematically compare multiple OpenEO backends across different dimensions.

Multi-Backend Service Monitoring
---------------------------------

Set up continuous monitoring of multiple backends:

.. code-block:: bash

   # Create comprehensive backend list
   cat > all_backends.csv << EOF
   url
   https://openeo.dataspace.copernicus.eu/.well-known/openeo
   https://openeo.vito.be/.well-known/openeo
   https://earthengine.openeo.org/.well-known/openeo
   https://openeo-dev.eodc.eu/.well-known/openeo
   https://openeo.dataspace.copernicus.eu/.well-known/openeo
   EOF

   # Monitor services hourly (add to cron)
   openeobench service -i all_backends.csv -o monitoring/ --append

Performance Comparison
----------------------

Compare execution performance across backends using the same scenario:

.. code-block:: bash

   # Create test scenario
   cat > performance_test.json << EOF
   {
     "process_graph": {
       "loadco1": {
         "process_id": "load_collection",
         "arguments": {
           "id": "SENTINEL2_L2A",
           "spatial_extent": {
             "west": 11.0,
             "east": 12.0,
             "north": 46.5,
             "south": 46.0
           },
           "temporal_extent": ["2024-01-01", "2024-01-31"]
         }
       },
       "bands": {
         "process_id": "filter_bands",
         "arguments": {
           "data": {"from_node": "loadco1"},
           "bands": ["B04", "B08"]
         }
       },
       "save": {
         "process_id": "save_result",
         "arguments": {
           "data": {"from_node": "bands"},
           "format": "GTiff"
         },
         "result": true
       }
     }
   }
   EOF

   # Run on multiple backends
   backends=(
     "https://openeo.dataspace.copernicus.eu"
     "https://openeo.vito.be"
     "https://earthengine.openeo.org"
   )

   backend_names=("cdse" "vito" "gee")

   for i in "${!backends[@]}"; do
     echo "Testing ${backend_names[$i]}..."
     openeobench run --api-url "${backends[$i]}" -i performance_test.json -o "performance/${backend_names[$i]}/"
   done

   # Generate performance comparison
   openeobench run-summary -i performance/*/ -o performance_comparison.csv

Process Coverage Analysis
-------------------------

Compare which processes are available across backends:

.. code-block:: bash

   # Check process availability
   for backend in "${backends[@]}"; do
     name=$(echo "$backend" | sed 's/.*\/\///;s/\..*$//')
     openeobench process --url "$backend" -o "processes/${name}_processes"
   done

   # Generate comprehensive process comparison
   openeobench process-summary processes/ --output process_coverage.md

Advanced Comparison Scenarios
-----------------------------

Test complex scenarios that stress different capabilities:

.. code-block:: json
   :caption: complex_scenario.json

   {
     "process_graph": {
       "loadco1": {
         "process_id": "load_collection",
         "arguments": {
           "id": "SENTINEL2_L2A",
           "spatial_extent": {
             "west": 16.0,
             "east": 16.5,
             "north": 48.5,
             "south": 48.0
           },
           "temporal_extent": ["2024-01-01", "2024-12-31"]
         }
       },
       "ndvi": {
         "process_id": "ndvi",
         "arguments": {
           "data": {"from_node": "loadco1"},
           "nir": "B08",
           "red": "B04"
         }
       },
       "temporal_mean": {
         "process_id": "reduce_dimension",
         "arguments": {
           "data": {"from_node": "ndvi"},
           "dimension": "t",
           "reducer": {
             "process_graph": {
               "mean": {
                 "process_id": "mean",
                 "arguments": {
                   "data": {"from_parameter": "data"}
                 },
                 "result": true
               }
             }
           }
         }
       },
       "save": {
         "process_id": "save_result",
         "arguments": {
           "data": {"from_node": "temporal_mean"},
           "format": "GTiff"
         },
         "result": true
       }
     }
   }

Data Quality Comparison
-----------------------

Compare output quality and characteristics:

.. code-block:: bash

   # Run complex scenario on all backends
   for i in "${!backends[@]}"; do
     echo "Running complex test on ${backend_names[$i]}..."
     openeobench run --api-url "${backends[$i]}" -i complex_scenario.json -o "quality/${backend_names[$i]}/"
   done

   # Analyze result quality
   openeobench result-summary quality/*/ --output quality_analysis.md

   # Create visual comparison
   openeobench visualize quality/*/ --output quality_comparison.md --format both

Automated Comparison Report
---------------------------

Generate a comprehensive comparison report:

.. code-block:: bash
   :caption: generate_comparison_report.sh

   #!/bin/bash
   
   echo "# OpenEO Backend Comparison Report" > comparison_report.md
   echo "Generated: $(date)" >> comparison_report.md
   echo "" >> comparison_report.md
   
   # Service availability
   echo "## Service Availability" >> comparison_report.md
   openeobench service-summary -i monitoring/ -o service_temp.md
   tail -n +3 service_temp.md >> comparison_report.md
   echo "" >> comparison_report.md
   
   # Process coverage
   echo "## Process Coverage" >> comparison_report.md
   openeobench process-summary processes/ --output process_temp.md
   tail -n +3 process_temp.md >> comparison_report.md
   echo "" >> comparison_report.md
   
   # Performance comparison
   echo "## Performance Comparison" >> comparison_report.md
   echo "### Timing Statistics" >> comparison_report.md
   echo "" >> comparison_report.md
   echo "| Backend | Avg Total Time | Avg Execution Time | Success Rate |" >> comparison_report.md
   echo "|---------|---------------|-------------------|--------------|" >> comparison_report.md
   
   # Parse performance data and add to table
   python3 << EOF
   import csv
   import sys
   
   try:
       with open('performance_comparison.csv', 'r') as f:
           reader = csv.DictReader(f)
           for row in reader:
               backend = row['filename'].split('/')[0]
               total_time = row.get('time_total', 'N/A')
               exec_time = row.get('time_job_execution', 'N/A')
               print(f"| {backend} | {total_time} | {exec_time} | - |")
   except FileNotFoundError:
       print("| No performance data | - | - | - |")
   EOF
   
   echo "" >> comparison_report.md
   
   # Quality analysis
   echo "## Quality Analysis" >> comparison_report.md
   if [ -f quality_analysis.md ]; then
       tail -n +3 quality_analysis.md >> comparison_report.md
   fi
   
   # Visual comparison
   echo "## Visual Comparison" >> comparison_report.md
   if [ -f quality_comparison.md ]; then
       grep -A 1000 "## Visual" quality_comparison.md >> comparison_report.md
   fi
   
   # Cleanup temp files
   rm -f service_temp.md process_temp.md
   
   echo "Comparison report generated: comparison_report.md"

Continuous Monitoring Setup
----------------------------

Set up automated backend monitoring:

.. code-block:: bash
   :caption: setup_monitoring.sh

   #!/bin/bash
   
   # Create monitoring directory
   mkdir -p /var/log/openeobench
   
   # Create monitoring script
   cat > monitor_backends.sh << 'EOF'
   #!/bin/bash
   cd /path/to/openeobench
   openeobench service -i all_backends.csv -o /var/log/openeobench/ --append
   
   # Weekly summary
   if [ $(date +%u) -eq 1 ]; then
     openeobench service-summary -i /var/log/openeobench/ -o "/var/log/openeobench/weekly_$(date +%Y%m%d).md"
   fi
   EOF
   
   chmod +x monitor_backends.sh
   
   # Add to crontab (run every hour)
   echo "0 * * * * /path/to/monitor_backends.sh" | crontab -

Results Analysis
----------------

The comparison will generate several output files:

* ``performance_comparison.csv`` - Timing statistics across backends
* ``process_coverage.md`` - Process availability matrix
* ``quality_analysis.md`` - Data quality comparison
* ``quality_comparison.md`` - Visual result comparison
* ``comparison_report.md`` - Comprehensive comparison report

This systematic approach allows you to:

1. **Monitor uptime** and response times continuously
2. **Compare performance** for identical workloads
3. **Assess process coverage** and compliance levels
4. **Evaluate output quality** and consistency
5. **Generate reports** for stakeholders and users
