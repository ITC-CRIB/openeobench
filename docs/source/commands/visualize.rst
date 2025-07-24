visualize
=========

Create visual matrices and reports of GeoTIFF results.

Syntax
------

.. code-block:: bash

   openeobench visualize [PATHS...] [OPTIONS]

Arguments
---------

.. option:: PATHS

   Input folders or individual files to visualize

Options
-------

.. option:: --output <file>

   Output file path

.. option:: --format <format>

   Output format: 'md', 'png', or 'both' (default: inferred from extension)

Examples
--------

Create visual matrix with both formats:

.. code-block:: bash

   openeobench visualize output/folder1 output/folder2 --output visualization.md --format both

Create PNG matrix only:

.. code-block:: bash

   openeobench visualize output/folder1 output/folder2 --output visualization.png --format png

Visualize individual files:

.. code-block:: bash

   openeobench visualize path/to/file1.tif path/to/file2.tif --output comparison.md --format both

Mixed input (folders and files):

.. code-block:: bash

   openeobench visualize output/folder1 path/to/specific_file.tif --output mixed_visualization.md

Dependencies
------------

* ``gdal`` - Geospatial data processing
* ``matplotlib`` - Plotting and visualization

Output Formats
--------------

* **Markdown** (``.md``): Reports with embedded image matrices and statistics
* **PNG** (``.png``): Matrix visualizations in image format
* **Both**: Generate both markdown and PNG outputs
* **Individual PNGs**: Separate images for each input GeoTIFF

.. note::
   Input can be folders containing GeoTIFF files, individual GeoTIFF files, or a combination of both.
