Visualization
=============

Create visual matrices and reports of GeoTIFF results.

Basic Usage
-----------

Create visual matrix of GeoTIFF results with format options:

.. code-block:: bash

   openeobench visualize output/folder1 output/folder2 --output visualization.md --format both

Create only PNG matrix:

.. code-block:: bash

   openeobench visualize output/folder1 output/folder2 --output visualization.png --format png

Create only markdown report:

.. code-block:: bash

   openeobench visualize output/folder1 output/folder2 --output visualization.md --format md

Visualize individual TIFF files:

.. code-block:: bash

   openeobench visualize path/to/file1.tif path/to/file2.tif --output comparison.md --format both

Mix folders and individual files:

.. code-block:: bash

   openeobench visualize output/folder1 path/to/specific_file.tif --output mixed_visualization.md

Output Formats
--------------

**Markdown Reports**
   Human-readable reports with embedded image matrices and statistics

**PNG Matrix Visualizations**
   Single image showing all results in a visual matrix format

**Individual PNG Files**
   Separate PNG files for each GeoTIFF input

**Mixed Input Support**
   Support for folders, individual files, or combinations of both

Dependencies
------------

The visualization functionality requires:

* GDAL - For geospatial data processing
* matplotlib - For plotting and image generation

.. note::
   CSV statistics output has been removed as of recent updates. Use markdown or PNG formats for visualization output.
