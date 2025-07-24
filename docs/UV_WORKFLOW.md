# Documentation Development with UV

This guide covers documentation development workflows using `uv` package manager.

## Quick Start

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and setup project
git clone https://github.com/ITC-CRIB/openeobench.git
cd openeobench

# Install all dependencies including docs
uv sync --extra docs

# Build documentation
cd docs
make html

# Start live documentation server
make livehtml
```

## Development Workflow

### 1. Environment Management

```bash
# Create and sync virtual environment
uv sync

# Add new documentation dependency
uv add --group docs sphinx-copybutton

# Install development dependencies
uv sync --extra dev --extra docs

# Lock dependencies
uv lock
```

### 2. Building Documentation

```bash
# Quick build
cd docs && make html

# Clean build (removes all cached files)
cd docs && make clean html

# Build with verbose output
cd docs && uv run sphinx-build -v source build/html

# Check for warnings
cd docs && uv run sphinx-build -W source build/html
```

### 3. Live Development

```bash
# Start live reload server
cd docs && make livehtml

# Custom live reload with specific host/port
cd docs && uv run sphinx-autobuild source build/html --host 0.0.0.0 --port 8080

# Live reload with specific files to watch
cd docs && uv run sphinx-autobuild source build/html --watch ../openeobench/
```

### 4. Quality Checks

```bash
# Check for broken links
cd docs && uv run sphinx-build -b linkcheck source build/linkcheck

# Spell checking (if sphinxcontrib-spelling is installed)
cd docs && uv run sphinx-build -b spelling source build/spelling

# Check coverage of API documentation
cd docs && uv run sphinx-build -b coverage source build/coverage
```

## Advanced Usage

### Custom Build Commands

Create custom commands in your shell profile or project scripts:

```bash
# Build and open documentation
docs-build() {
    cd docs && make clean html && open build/html/index.html
}

# Quick syntax check
docs-check() {
    cd docs && uv run sphinx-build -nW --keep-going source build/html
}

# Build specific format
docs-pdf() {
    cd docs && uv run sphinx-build -b latex source build/latex
    cd build/latex && make
}
```

### Integration with VS Code

Add to your `.vscode/tasks.json`:

```json
{
    "version": "2.0.0",
    "tasks": [
        {
            "label": "Build Documentation",
            "type": "shell",
            "command": "uv",
            "args": ["run", "sphinx-build", "docs/source", "docs/build/html"],
            "group": "build",
            "presentation": {
                "echo": true,
                "reveal": "always",
                "focus": false,
                "panel": "shared"
            },
            "problemMatcher": []
        },
        {
            "label": "Live Documentation",
            "type": "shell",
            "command": "uv",
            "args": ["run", "sphinx-autobuild", "docs/source", "docs/build/html"],
            "group": "build",
            "isBackground": true,
            "presentation": {
                "echo": true,
                "reveal": "always",
                "focus": false,
                "panel": "shared"
            }
        }
    ]
}
```

### CI/CD Integration

For GitHub Actions with uv:

```yaml
- name: Setup uv
  run: |
    curl -LsSf https://astral.sh/uv/install.sh | sh
    echo "$HOME/.cargo/bin" >> $GITHUB_PATH

- name: Install dependencies
  run: uv sync --extra docs

- name: Build documentation
  run: |
    cd docs
    uv run sphinx-build -W --keep-going source build/html
```

## Troubleshooting

### Common Issues

1. **Module not found errors**:
   ```bash
   # Ensure environment is synced
   uv sync --extra docs
   
   # Check installed packages
   uv pip list
   ```

2. **GDAL import errors**:
   ```bash
   # Install system GDAL
   sudo apt-get install gdal-bin libgdal-dev
   
   # Reinstall Python GDAL
   uv pip install --force-reinstall gdal
   ```

3. **Sphinx build failures**:
   ```bash
   # Build with verbose output
   cd docs && uv run sphinx-build -v source build/html
   
   # Check configuration
   cd docs && uv run python -c "import conf; print(conf)"
   ```

### Performance Optimization

```bash
# Use parallel builds
cd docs && uv run sphinx-build -j auto source build/html

# Enable incremental builds
cd docs && uv run sphinx-build -E source build/html

# Profile build time
cd docs && uv run sphinx-build -v -T source build/html
```

## Best Practices

1. **Lock your dependencies**:
   ```bash
   uv lock
   git add uv.lock
   ```

2. **Use dependency groups**:
   - Keep docs dependencies separate from runtime dependencies
   - Use `--extra docs` for documentation builds only

3. **Regular maintenance**:
   ```bash
   # Update dependencies
   uv sync --upgrade
   
   # Audit for security issues
   uv audit
   ```

4. **Version compatibility**:
   ```bash
   # Check Python version compatibility
   uv run python --version
   
   # Test with different Python versions
   uv python install 3.11 3.12
   uv sync --python 3.11
   ```

This workflow leverages uv's fast dependency resolution and virtual environment management for efficient documentation development.
