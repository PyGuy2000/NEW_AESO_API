# Core Module - Platform Configuration

This module provides platform-aware configuration and file handling for the NEW_AESO_API project, enabling seamless operation across Windows and WSL2 environments.

## Components

### 1. `platform_config.py`
Handles platform detection and configuration:
- Detects Windows, WSL2, or Linux
- Manages environment-specific paths
- Loads application settings from `.env` file
- Provides path conversion between Windows and WSL2

### 2. `file_handler.py`
Provides platform-agnostic file operations:
- Read/write CSV files
- Read/write JSON files
- File system operations (list, search, delete)
- Automatic path resolution

## Usage

### Basic Import
```python
from core import platform_config, file_handler
```

### Reading Files
```python
# Read CSV with automatic path resolution
df = file_handler.read_csv('output/data.csv')

# Read JSON
config = file_handler.read_json('config/settings.json')
```

### Writing Files
```python
# Write CSV
file_handler.write_csv(df, 'output/results.csv', index=False)

# Write JSON
file_handler.write_json(data, 'output/results.json')
```

### Path Resolution
```python
# Get platform-appropriate paths
paths = platform_config.get_base_paths()
output_dir = paths['output']
input_dir = paths['input']

# Resolve relative paths
full_path = platform_config.resolve_path('output/subfolder/file.csv')
```

### Application Settings
```python
# Get current settings
settings = platform_config.get_settings()
csv_output = settings['CSV_OUTPUT']

# Update settings at runtime
platform_config.update_settings('CSV_OUTPUT', False)
```

## Environment Variables

Configure in `.env` file:

```env
# IDE Option
IDE_OPTION=vscode

# Platform-specific paths
GBL_HOME_DIR=/home/rob_kaz/python/projects/NEW_AESO_API
GBL_OFFICE_DIR=C:/Users/kaczanor/OneDrive - Enbridge Inc/Documents/Python/NEW_AESO_API

# Application settings
CSV_OUTPUT=True
SQLITE_OUTPUT=False
DELETE_EXISTING_OUTPUT_FILES=False
```

## Testing

Run the test script to verify configuration:
```bash
python test_platform.py
```

This will display:
- Detected platform
- Base directory paths
- Application settings
- File system access

## How It Works

1. **Platform Detection**: Automatically detects if running on Windows, WSL2, or Linux
2. **Path Resolution**: Converts paths between Windows (`C:/Users/...`) and WSL2 (`/home/...`) formats
3. **Environment Detection**: Uses `.env` variables to determine home vs office location
4. **Automatic Fallback**: If environment variables aren't set, uses intelligent defaults based on platform

## Migration from Old Code

To migrate existing code:

**Before:**
```python
output_folder = 'C:/Users/kaczanor/output/'
df = pd.read_csv(output_folder + 'data.csv')
```

**After:**
```python
from core import file_handler
df = file_handler.read_csv('output/data.csv')
```

The file handler automatically resolves the correct path for your platform!
