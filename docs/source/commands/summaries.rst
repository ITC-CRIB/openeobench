run-summary
===========

Generate timing statistics from workflow executions.

Syntax
------

.. code-block:: bash

   openeobench run-summary [OPTIONS]

Options
-------

.. option:: -i, --input <paths>

   Input result folders or files (multiple allowed)

.. option:: -o, --output <file>

   Output CSV file

Examples
--------

Generate timing summary:

.. code-block:: bash

   openeobench run-summary -i output/folder1 output/folder2 -o timing_summary.csv

result-summary
==============

Generate comprehensive statistics from workflow outputs.

Syntax
------

.. code-block:: bash

   openeobench result-summary [PATHS...] [OPTIONS]

Arguments
---------

.. option:: PATHS

   Input folders or files to analyze

Options
-------

.. option:: --output <file>

   Output file path

.. option:: --format <format>

   Output format: 'csv' or 'md' (default: inferred from extension)

Examples
--------

Generate CSV statistics:

.. code-block:: bash

   openeobench result-summary output/folder1 output/folder2 --output file_stats.csv

Generate Markdown report:

.. code-block:: bash

   openeobench result-summary output/folder1 output/folder2 --output file_stats.md --format md
