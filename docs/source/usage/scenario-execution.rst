Scenario Execution
==================

Run OpenEO workflows on backends.

Basic Usage
-----------

.. code-block:: bash

   # Run OpenEO scenario on a backend
   openeobench run --api-url https://openeo.dataspace.copernicus.eu -i scenario.json -o results/

Output Structure
----------------

Results are organized in timestamped folders containing:

* **Process graph (JSON)** - The executed OpenEO process graph
* **Job metadata (JSON)** - Timing data, job status, and execution details
* **Output files** - Downloaded results (GeoTIFF, etc.)

Example output structure:

.. code-block:: text

   results/
   ├── 2025-01-15_14-30-25/
   │   ├── processgraph.json
   │   ├── results.json
   │   └── output_data.tif
   └── 2025-01-15_15-45-12/
       ├── processgraph.json
       ├── results.json
       └── output_data.tif

Scenario Format
---------------

Scenarios are defined as JSON files containing OpenEO process graphs. Example:

.. code-block:: json

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
           "temporal_extent": ["2018-01-01", "2018-02-01"]
         }
       },
       "save": {
         "process_id": "save_result",
         "arguments": {
           "data": {"from_node": "loadco1"},
           "format": "GTiff"
         },
         "result": true
       }
     }
   }
