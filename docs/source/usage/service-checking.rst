Service Checking
================

Test OpenEO endpoint availability and response times.

Basic Usage
-----------

Check endpoints from CSV file (creates new timestamped file):

.. code-block:: bash

   openeobench service -i endpoints.csv -o results/

Check endpoints and append to daily file:

.. code-block:: bash

   openeobench service -i endpoints.csv -o results/ --append

Check a single URL (creates new timestamped file):

.. code-block:: bash

   openeobench service -u https://openeo.dataspace.copernicus.eu/.well-known/openeo -o results/

Check a single URL and append to daily file:

.. code-block:: bash

   openeobench service -u https://openeo.dataspace.copernicus.eu/.well-known/openeo -o results/ --append

Output Format
-------------

Results are saved as CSV files with the following columns:

* ``url`` - The tested endpoint URL
* ``timestamp`` - When the test was performed
* ``response_time`` - Response time in seconds
* ``status_code`` - HTTP status code
* ``error_msg`` - Error message if request failed
* ``content_size`` - Size of response content

Files are organized as:

* ``outputs/YYYY-MM-DD.csv`` - Daily aggregated results
* ``outputs/YYYY-MM-DD_single.csv`` - Single URL test results
