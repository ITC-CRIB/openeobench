# Setting Up Documentation with Read the Docs

This guide helps you set up OpenEO Bench documentation with Read the Docs using `uv`.

## Local Development

### Prerequisites

- Python 3.11+
- Git
- uv package manager

### Setup

1. **Install uv** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Install documentation dependencies**:
   ```bash
   # Install all dependencies including docs extras
   uv sync --extra docs
   ```

3. **Build documentation locally**:
   ```bash
   cd docs
   make html
   ```

4. **View documentation**:
   ```bash
   # Open in browser
   open build/html/index.html
   
   # Or serve with Python
   cd build/html
   uv run python -m http.server 8000
   # Visit http://localhost:8000
   ```

5. **Live reload during development**:
   ```bash
   # Install sphinx-autobuild (included in docs extras)
   uv add --group docs sphinx-autobuild
   
   # Start live server
   cd docs
   make livehtml
   # Visit http://127.0.0.1:8000
   ```

## Read the Docs Setup

### 1. Create Read the Docs Account

1. Go to [Read the Docs](https://readthedocs.org/)
2. Sign up/in with your GitHub account
3. Import your repository

### 2. Configure Project

1. **Import Project**:
   - Go to your RTD dashboard
   - Click "Import a Project"
   - Select your repository
   - Set project name: `openeobench`

2. **Configure Settings**:
   - **Language**: Python
   - **Programming Language**: Python
   - **Repository URL**: Your GitHub repository URL
   - **Default Branch**: `main`

3. **Advanced Settings**:
   - **Python Configuration File**: `pyproject.toml`
   - **Use system packages**: Checked (for GDAL)
   - **Python Interpreter**: `python3.11`

### 3. Environment Configuration

The `.readthedocs.yaml` file in the repository root configures:
- Python version (3.11)
- Sphinx configuration path
- Build requirements
- Output formats (HTML, PDF, ePub)

### 4. Build and Deploy

1. **Trigger Build**:
   - Push changes to main branch
   - Or manually trigger from RTD dashboard

2. **Check Build Status**:
   - Monitor build logs in RTD dashboard
   - Fix any build errors

3. **Access Documentation**:
   - Main URL: `https://openeobench.readthedocs.io/`
   - Version-specific: `https://openeobench.readthedocs.io/en/latest/`

## Documentation Structure

```
docs/
├── source/
│   ├── _static/          # Static files (images, CSS)
│   ├── _templates/       # Custom Sphinx templates
│   ├── api/             # API documentation
│   ├── commands/        # Command reference
│   ├── examples/        # Usage examples
│   ├── usage/           # User guides
│   ├── conf.py          # Sphinx configuration
│   ├── index.rst        # Main page
│   ├── installation.rst # Installation guide
│   ├── overview.rst     # Project overview
│   ├── contributing.rst # Contribution guide
│   └── changelog.rst    # Version history
├── requirements.txt     # Doc build dependencies
├── Makefile            # Build commands (Unix)
└── make.bat           # Build commands (Windows)
```

## Customization

### Theme Configuration

Edit `docs/source/conf.py`:

```python
# Change theme
html_theme = 'sphinx_rtd_theme'

# Theme options
html_theme_options = {
    'logo_only': False,
    'display_version': True,
    'prev_next_buttons_location': 'bottom',
    'style_external_links': False,
    'collapse_navigation': True,
    'sticky_navigation': True,
    'navigation_depth': 4,
    'includehidden': True,
    'titles_only': False
}
```

### Custom CSS/JS

Add files to `docs/source/_static/` and reference in `conf.py`:

```python
html_static_path = ['_static']
html_css_files = ['custom.css']
html_js_files = ['custom.js']
```

### Logo and Favicon

```python
html_logo = '_static/logo.png'
html_favicon = '_static/favicon.ico'
```

## Automation

### GitHub Actions

The `.github/workflows/docs.yml` file provides:
- Automatic builds on push
- Deployment to GitHub Pages
- Build status checks

### Read the Docs Webhooks

RTD automatically builds when:
- Code is pushed to configured branches
- Pull requests are created
- Tags are created

## Troubleshooting

### Common Build Issues

1. **GDAL Import Errors**:
   - Ensure system packages are enabled in RTD
   - Check GDAL version compatibility

2. **Missing Dependencies**:
   - Verify `docs/requirements.txt` is complete
   - Check pyproject.toml optional dependencies

3. **Sphinx Warnings**:
   - Fix all warnings to prevent build failures
   - Use `fail_on_warning: false` in `.readthedocs.yaml` temporarily

### Build Commands

```bash
# Clean build
make clean && make html

# Check for broken links
uv run make linkcheck

# Check spelling (requires sphinxcontrib-spelling)
uv run make spelling

# Build PDF
uv run make latexpdf
```

## Maintenance

### Regular Tasks

1. **Update Dependencies**:
   ```bash
   uv sync --upgrade
   # Check for outdated packages
   uv pip list --outdated
   ```

2. **Review Documentation**:
   - Check for outdated information
   - Update examples and screenshots
   - Fix broken links

3. **Monitor Analytics**:
   - Use RTD analytics dashboard
   - Track popular pages
   - Identify areas for improvement

### Versioning

RTD supports multiple versions:
- **latest**: Development version (main branch)
- **stable**: Latest release tag
- **Tagged versions**: Specific releases

Configure in RTD project settings under "Versions".

## Resources

- [Read the Docs Documentation](https://docs.readthedocs.io/)
- [Sphinx Documentation](https://www.sphinx-doc.org/)
- [reStructuredText Primer](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html)
- [MyST Parser](https://myst-parser.readthedocs.io/) (Markdown support)
