Installation
============

System Requirements
-------------------

* Python 3.13+ (as specified in pyproject.toml)
* GDAL system libraries (for geospatial functionality)

Using uv (recommended)
----------------------

.. code-block:: bash

   # Install uv
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # Clone and setup
   git clone <repository-url>
   cd openeobench

   # Install all dependencies (includes GDAL, OpenEO, etc.)
   uv sync

   # Run commands
   uv run python openeobench --help

Using pip
---------

Install all dependencies (full functionality)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   git clone <repository-url>
   cd openeobench

   # Install from pyproject.toml (includes all dependencies)
   pip install -e .

   # Or install specific requirements
   pip install -r requirements.txt

   python openeobench --help

.. note::
   When installing GDAL, make sure that the system GDAL library version and the Python GDAL package version are compatible. Mismatched versions can cause import errors or runtime issues. It is recommended to install the system GDAL libraries first, then install the matching Python GDAL version (e.g., using ``pip install gdal==<version>`` where ``<version>`` matches your system GDAL).

Python Dependencies
-------------------

Core dependencies (from pyproject.toml):

* ``requests>=2.32.4`` - HTTP client for API calls
* ``openeo>=0.42.1`` - OpenEO Python client
* ``gdal[numpy]==3.8.4`` - Geospatial data processing
* ``numpy>=2.3.0`` - Numerical computing
* ``matplotlib>=3.10.3`` - Plotting and visualization
* ``rioxarray>=0.19.0`` - Raster data handling

Installation Notes
------------------

* **GDAL requirement**: May require system-level GDAL installation on some platforms
* All commands require the full dependency set for proper functionality
