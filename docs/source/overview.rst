Overview
========

openEObench provides comprehensive functionality for testing and benchmarking OpenEO backends:

Core Features
-------------

* **Service Checking**: Test endpoint availability and response times
* **Scenario Execution**: Run OpenEO workflows on backends  
* **Run Summaries**: Analyze timing statistics from workflow executions
* **Result Summaries**: Generate comprehensive statistics from workflow outputs
* **Service Summaries**: Generate performance reports from endpoint checks
* **Process Analysis**: Check OpenEO process availability and compliance across backends
* **Process Summaries**: Generate compliance reports for process implementations
* **Visualization**: Create visual matrices and reports of GeoTIFF results

Architecture
------------

openEObench is designed as a modular command-line tool that can:

1. **Test Backend Availability**: Monitor OpenEO service endpoints
2. **Execute Workflows**: Run predefined or custom OpenEO scenarios
3. **Analyze Results**: Generate comprehensive reports and statistics
4. **Validate Compliance**: Check process implementations against OpenEO profiles

OpenEO Process Profiles
-----------------------

The process compliance checking is based on the official OpenEO API specification process profiles:

**L1 (Basic)**
   Essential processes for basic data access and output:
   ``load_collection``, ``save_result``, ``filter_bbox``, ``filter_temporal``, 
   ``reduce_dimension``, ``apply``, ``linear_scale_range``

**L2 (EO Data Manipulation)**
   Earth observation specific data processing:
   ``ndvi``, ``evi``, ``aggregate_temporal``, ``resample_spatial``, ``merge_cubes``, 
   ``apply_dimension``, ``array_element``, ``clip``, ``mask``, ``filter_bands``

**L3 (Mathematical Operations)**
   Mathematical and statistical functions:
   ``add``, ``subtract``, ``multiply``, ``divide``, ``absolute``, ``sqrt``, ``power``, 
   ``exp``, ``ln``, ``log``, ``sin``, ``cos``, ``tan``, ``arcsin``, ``arccos``, 
   ``arctan``, ``min``, ``max``, ``mean``, ``median``, ``sum``, ``product``, 
   ``count``, ``sd``, ``variance``

**L4 (Advanced Analysis)**
   Advanced algorithms and machine learning:
   ``fit_curve``, ``predict_curve``, ``ml_fit``, ``ml_predict``, ``sar_backscatter``, 
   ``atmospheric_correction``, ``cloud_detection``, ``create_data_cube``
