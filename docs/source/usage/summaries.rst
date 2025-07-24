Summaries and Reports
=====================

Generate various types of summary reports from execution results.

Service Summary
---------------

Generate performance reports from endpoint check results:

.. code-block:: bash

   # Generate statistics summary from folder (CSV output)
   openeobench service-summary -i results/ -o summary.csv

   # Generate statistics summary from folder (Markdown output)
   openeobench service-summary -i results/ -o summary.md

   # Generate statistics from single CSV file
   openeobench service-summary -i results/2025-06-26.csv -o summary.md

**CSV Output Columns:**
   * ``url`` - Endpoint URL
   * ``availability`` - Availability percentage
   * ``avg_response_time`` - Average response time
   * ``stddev_response_time`` - Response time standard deviation
   * ``latency`` - Network latency
   * ``latency_stddev`` - Latency standard deviation

**Markdown Output:**
   Formatted document with statistics tables and performance analysis.

Run Summary
-----------

Generate timing statistics from workflow executions:

.. code-block:: bash

   # Generate timing summary from run results
   openeobench run-summary -i output/folder1 output/folder2 -o timing_summary.csv

**Output Columns:**
   * ``filename`` - Result file identifier
   * ``time_submit`` - Job submission time
   * ``time_submit_stddev`` - Submission time standard deviation
   * ``time_job_execution`` - Job execution time
   * ``time_job_execution_stddev`` - Execution time standard deviation
   * ``time_download`` - Download time
   * ``time_download_stddev`` - Download time standard deviation
   * ``time_processing`` - Processing time
   * ``time_processing_stddev`` - Processing time standard deviation
   * ``time_queue`` - Queue time
   * ``time_queue_stddev`` - Queue time standard deviation
   * ``time_total`` - Total time
   * ``time_total_stddev`` - Total time standard deviation

Result Summary
--------------

Generate comprehensive statistics from workflow outputs:

.. code-block:: bash

   # Generate file statistics summary from run results (CSV)
   openeobench result-summary output/folder1 output/folder2 --output file_stats.csv

   # Generate file statistics summary from run results (Markdown)
   openeobench result-summary output/folder1 output/folder2 --output file_stats.md --format md

**Output:**
   Comprehensive statistics about generated files, data types, sizes, and processing results.
