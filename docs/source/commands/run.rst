run
===

Execute OpenEO scenarios on backends.

Syntax
------

.. code-block:: bash

   openeobench run [OPTIONS]

Options
-------

.. option:: --api-url <url>

   OpenEO backend API URL

.. option:: -i, --input <file>

   Scenario JSON file to execute

.. option:: -o, --output <directory>

   Output directory for results

Examples
--------

Run scenario on backend:

.. code-block:: bash

   openeobench run --api-url https://openeo.dataspace.copernicus.eu -i scenario.json -o results/

Dependencies
------------

* ``openeo`` - OpenEO Python client
* ``requests`` - HTTP client for API calls

Output Structure
----------------

Results are saved in timestamped folders containing:

* ``processgraph.json`` - The executed process graph
* ``results.json`` - Job metadata and timing information
* Output files (e.g., ``*.tif``) - Downloaded results
