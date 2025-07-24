service
=======

Check endpoint availability and response times.

Syntax
------

.. code-block:: bash

   openeobench service [OPTIONS]

Options
-------

.. option:: -i, --input <file>

   CSV file containing endpoints to check

.. option:: -u, --url <url>

   Single URL to check

.. option:: -o, --output <directory>

   Output directory for results

.. option:: --append

   Append results to daily file instead of creating new timestamped file

Examples
--------

Check endpoints from CSV file:

.. code-block:: bash

   openeobench service -i endpoints.csv -o results/

Check single URL with append:

.. code-block:: bash

   openeobench service -u https://openeo.example.com/.well-known/openeo -o results/ --append

service-summary
===============

Generate performance reports from endpoint check results.

Syntax
------

.. code-block:: bash

   openeobench service-summary [OPTIONS]

Options
-------

.. option:: -i, --input <path>

   Input folder containing CSV files or single CSV file

.. option:: -o, --output <file>

   Output file (CSV or Markdown based on extension)

Examples
--------

Generate CSV summary:

.. code-block:: bash

   openeobench service-summary -i results/ -o summary.csv

Generate Markdown report:

.. code-block:: bash

   openeobench service-summary -i results/2025-06-26.csv -o summary.md
