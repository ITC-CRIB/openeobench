Process Compliance
==================

Check OpenEO process availability and compliance across backends.

Process Checking
----------------

Check process compliance for a single backend:

.. code-block:: bash

   openeobench process --url https://openeo.vito.be/openeo/1.1 -o process_results.csv

Check process compliance for multiple backends:

.. code-block:: bash

   openeobench process -i backends.csv -o process_compliance.csv

Output Files
------------

The process checking generates two types of output files:

**CSV File (`.csv`)**
   Contains process compliance analysis with columns:
   
   * ``process`` - Process name
   * ``level`` - OpenEO profile level (L1-L4)
   * ``status`` - Availability status
   * ``compatibility`` - Compatibility assessment
   * ``reason`` - Explanation for status/compatibility

**JSON File (`.json`)**
   Contains the raw ``/processes`` endpoint response for detailed analysis.

Process Summary
---------------

Generate compliance reports for process implementations:

.. code-block:: bash

   # Generate process compliance summary (CSV)
   openeobench process-summary process_results/ --output process_summary.csv --format csv

   # Generate process compliance summary (Markdown)  
   openeobench process-summary process_results/ --output process_summary.md --format md

**CSV Output Columns:**
   * ``backend`` - Backend identifier
   * ``l1_available`` - L1 processes available
   * ``l1_compliance_rate`` - L1 compliance percentage
   * ``l2_available`` - L2 processes available
   * ``l2_compliance_rate`` - L2 compliance percentage
   * ``l3_available`` - L3 processes available
   * ``l3_compliance_rate`` - L3 compliance percentage
   * ``l4_available`` - L4 processes available
   * ``l4_compliance_rate`` - L4 compliance percentage

**Markdown Output:**
   Formatted document with compliance analysis tables and cross-backend comparisons.

OpenEO Profile Levels
---------------------

Process compliance is checked against these OpenEO API specification profiles:

**L1 (Basic)**
   Essential processes for basic data access and output

**L2 (EO Data Manipulation)**
   Earth observation specific data processing

**L3 (Mathematical Operations)**
   Mathematical and statistical functions

**L4 (Advanced Analysis)**
   Advanced algorithms and machine learning

See the :doc:`../overview` section for detailed process lists for each level.
