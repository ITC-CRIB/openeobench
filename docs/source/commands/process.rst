process
=======

Check OpenEO process availability and compliance across backends.

Syntax
------

.. code-block:: bash

   openeobench process [OPTIONS]

Options
-------

.. option:: --url <url>

   Single backend URL to check

.. option:: -i, --input <file>

   CSV file containing multiple backend URLs

.. option:: -o, --output <file>

   Output file prefix (generates .csv and .json files)

Examples
--------

Check single backend:

.. code-block:: bash

   openeobench process --url https://openeo.vito.be/openeo/1.1 -o process_results

Check multiple backends:

.. code-block:: bash

   openeobench process -i backends.csv -o process_compliance

Dependencies
------------

* ``requests`` - HTTP client for API calls

Output Files
------------

* ``{output}.csv`` - Process compliance analysis
* ``{output}.json`` - Raw processes endpoint response

process-summary
===============

Generate compliance reports for process implementations.

Syntax
------

.. code-block:: bash

   openeobench process-summary [OPTIONS]

Options
-------

.. option:: -i, --input <path>

   Input folder or file containing process results

.. option:: --output <file>

   Output file path

.. option:: --format <format>

   Output format: 'csv' or 'md' (default: inferred from extension)

Examples
--------

Generate CSV summary:

.. code-block:: bash

   openeobench process-summary process_results/ --output process_summary.csv --format csv

Generate Markdown report:

.. code-block:: bash

   openeobench process-summary process_results/ --output process_summary.md --format md
