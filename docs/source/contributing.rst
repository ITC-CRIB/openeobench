Contributing
============

We welcome contributions to openEObench! This guide will help you get started.

Development Setup
-----------------

1. **Fork and clone the repository**:

   .. code-block:: bash

      git clone https://github.com/yourusername/openeobench.git
      cd openeobench

2. **Set up development environment**:

   .. code-block:: bash

      # Using uv (recommended)
      uv sync --dev

      # Or using pip
      pip install -e ".[dev]"

3. **Install pre-commit hooks**:

   .. code-block:: bash

      pre-commit install

Code Style
----------

We follow standard Python coding conventions:

* **PEP 8** for code style
* **Black** for code formatting
* **isort** for import sorting
* **flake8** for linting

Run code formatting:

.. code-block:: bash

   black .
   isort .
   flake8 .

Testing
-------

We use pytest for testing. Run the test suite:

.. code-block:: bash

   # Run all tests
   pytest

   # Run with coverage
   pytest --cov=openeobench

   # Run specific test file
   pytest tests/test_service_checking.py

Writing Tests
~~~~~~~~~~~~~

When adding new features, please include tests:

.. code-block:: python

   import pytest
   from openeobench import service_checker

   def test_service_check():
       """Test service availability checking."""
       result = service_checker.check_endpoint("https://example.com")
       assert result is not None
       assert "response_time" in result

Adding New Features
-------------------

1. **Create a feature branch**:

   .. code-block:: bash

      git checkout -b feature/your-feature-name

2. **Implement your feature**:
   
   * Add the functionality to the appropriate module
   * Include comprehensive docstrings
   * Add command-line interface if applicable
   * Include error handling

3. **Add tests**:
   
   * Unit tests for individual functions
   * Integration tests for complete workflows
   * Test edge cases and error conditions

4. **Update documentation**:
   
   * Add docstrings to all new functions/classes
   * Update relevant .rst files in ``docs/source/``
   * Add usage examples if appropriate

5. **Submit a pull request**:
   
   * Describe the feature and its benefits
   * Include test results
   * Reference any related issues

Documentation
-------------

Documentation is built using Sphinx. To build locally:

.. code-block:: bash

   cd docs
   make html

The documentation will be available in ``docs/build/html/``.

Documentation Guidelines
~~~~~~~~~~~~~~~~~~~~~~~~

* Use clear, concise language
* Include code examples for all features
* Document all parameters and return values
* Add cross-references between related sections

Release Process
---------------

1. **Update version** in ``pyproject.toml``
2. **Update changelog** with new features and fixes
3. **Create release tag**:

   .. code-block:: bash

      git tag -a v1.0.0 -m "Release version 1.0.0"
      git push origin v1.0.0

4. **Build and publish**:

   .. code-block:: bash

      uv build
      uv publish

Bug Reports
-----------

When reporting bugs, please include:

* **Environment details** (OS, Python version, dependency versions)
* **Minimal reproduction example**
* **Expected vs actual behavior**
* **Error messages and stack traces**
* **Relevant configuration files**

Feature Requests
----------------

For new feature requests:

* **Describe the use case** and problem to solve
* **Propose a solution** or approach
* **Consider backwards compatibility**
* **Provide examples** of how it would be used

Code of Conduct
---------------

We are committed to providing a welcoming and inclusive environment for all contributors. Please:

* Be respectful and professional
* Welcome newcomers and help them get started
* Focus on constructive feedback
* Respect different viewpoints and experiences

Getting Help
------------

If you need help:

* **Check the documentation** first
* **Search existing issues** on GitHub
* **Ask questions** in GitHub discussions
* **Join our community** channels (if available)

Development Workflow
--------------------

1. **Check existing issues** to avoid duplicate work
2. **Create an issue** for significant changes
3. **Fork the repository** and create a feature branch
4. **Make your changes** with tests and documentation
5. **Run the test suite** and ensure all tests pass
6. **Submit a pull request** with a clear description
7. **Address review feedback** promptly
8. **Celebrate** when your contribution is merged! 🎉

Thank you for contributing to openEObench!
