Basic Workflow
==============

This example demonstrates a complete openEObench workflow from service checking to result visualization.

Step 1: Check Service Availability
-----------------------------------

First, create a CSV file with backend endpoints:

.. code-block:: text
   :caption: backends.csv

   url
   https://openeo.dataspace.copernicus.eu/.well-known/openeo
   https://openeo.vito.be/.well-known/openeo
   https://earthengine.openeo.org/.well-known/openeo

Check service availability:

.. code-block:: bash

   openeobench service -i backends.csv -o service_results/

This creates timestamped CSV files in ``service_results/`` with response times and status codes.

Step 2: Run Scenarios
----------------------

Execute a simple NDVI calculation scenario:

.. code-block:: json
   :caption: ndvi_scenario.json

   {
     "process_graph": {
       "loadco1": {
         "process_id": "load_collection",
         "arguments": {
           "id": "SENTINEL2_L2A",
           "spatial_extent": {
             "west": 16.1,
             "east": 16.6,
             "north": 48.6,
             "south": 47.9
           },
           "temporal_extent": ["2024-06-01", "2024-06-30"]
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
       "save": {
         "process_id": "save_result",
         "arguments": {
           "data": {"from_node": "ndvi"},
           "format": "GTiff"
         },
         "result": true
       }
     }
   }

Run the scenario on multiple backends:

.. code-block:: bash

   # CDSE backend
   openeobench run --api-url https://openeo.dataspace.copernicus.eu -i ndvi_scenario.json -o results/cdse/

   # VITO backend  
   openeobench run --api-url https://openeo.vito.be -i ndvi_scenario.json -o results/vito/

Step 3: Analyze Results
-----------------------

Generate timing statistics:

.. code-block:: bash

   openeobench run-summary -i results/cdse/ results/vito/ -o timing_comparison.csv

Generate file statistics:

.. code-block:: bash

   openeobench result-summary results/cdse/ results/vito/ --output result_analysis.md

Step 4: Check Process Compliance
---------------------------------

Check which processes are available on each backend:

.. code-block:: bash

   openeobench process -i backends.csv -o process_compliance

Generate compliance summary:

.. code-block:: bash

   openeobench process-summary process_compliance/ --output compliance_report.md

Step 5: Visualize Results
-------------------------

Create visual comparison of outputs:

.. code-block:: bash

   openeobench visualize results/cdse/ results/vito/ --output comparison.md --format both

This generates:
- A markdown report with embedded visualizations
- PNG matrix showing all results side-by-side
- Individual PNG files for each result

Step 6: Generate Service Summary
---------------------------------

Create performance report from service checks:

.. code-block:: bash

   openeobench service-summary -i service_results/ -o service_performance.md

Complete Workflow Script
-------------------------

Here's a complete bash script that runs the entire workflow:

.. code-block:: bash
   :caption: benchmark_workflow.sh

   #!/bin/bash
   
   # Setup
   mkdir -p results/{cdse,vito} service_results
   
   # Step 1: Check services
   echo "Checking service availability..."
   openeobench service -i backends.csv -o service_results/
   
   # Step 2: Run scenarios
   echo "Running scenarios..."
   openeobench run --api-url https://openeo.dataspace.copernicus.eu -i ndvi_scenario.json -o results/cdse/
   openeobench run --api-url https://openeo.vito.be -i ndvi_scenario.json -o results/vito/
   
   # Step 3: Analyze results
   echo "Generating analysis reports..."
   openeobench run-summary -i results/cdse/ results/vito/ -o timing_comparison.csv
   openeobench result-summary results/cdse/ results/vito/ --output result_analysis.md
   
   # Step 4: Check compliance
   echo "Checking process compliance..."
   openeobench process -i backends.csv -o process_compliance
   openeobench process-summary process_compliance/ --output compliance_report.md
   
   # Step 5: Visualize
   echo "Creating visualizations..."
   openeobench visualize results/cdse/ results/vito/ --output comparison.md --format both
   
   # Step 6: Service summary
   echo "Generating service summary..."
   openeobench service-summary -i service_results/ -o service_performance.md
   
   echo "Workflow complete! Check the generated reports."

Expected Output Files
---------------------

After running the complete workflow, you'll have:

.. code-block:: text

   project/
   ├── service_results/
   │   └── 2025-07-24_14-30-25.csv
   ├── results/
   │   ├── cdse/
   │   │   └── 2025-07-24_14-35-12/
   │   │       ├── processgraph.json
   │   │       ├── results.json
   │   │       └── result.tif
   │   └── vito/
   │       └── 2025-07-24_14-40-05/
   │           ├── processgraph.json
   │           ├── results.json
   │           └── result.tif
   ├── process_compliance.csv
   ├── process_compliance.json
   ├── timing_comparison.csv
   ├── result_analysis.md
   ├── compliance_report.md
   ├── comparison.md
   ├── comparison.png
   └── service_performance.md
