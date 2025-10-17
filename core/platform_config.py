"""
Platform Configuration Module for NEW_AESO_API
Consolidates platform detection, IDE detection, and environment setup
Single source of truth for all platform/environment configurations
"""

import os
import sys
import platform
from pathlib import Path
from dotenv import load_dotenv

class PlatformConfig:
    """
    Handles:
    - Platform detection (Windows/WSL2/Linux)
    - IDE detection (vscode/kaggle/jupyter)
    - Environment detection (home/office)
    - Base directory setup
    - Application behaviour settings
    """
    
    def __init__(self):
        # Load environment variables first
        load_dotenv()
        
        # Core detection methods
        self._detect_platform()
        self._detect_ide()
        self._detect_environment()
        
        # Setup base directories
        self._setup_base_directories()

        # Load application settings
        self._load_application_settings()

        # Print configuration on initialization
        self.print_config()
    
    def _detect_platform(self):
        """Detect the current platform (Windows, WSL2, or Linux)"""
        system = platform.system()
        
        # Check if running in WSL2
        self.is_wsl2 = False
        if system == 'Linux':
            try:
                with open('/proc/version', 'r') as f:
                    if 'microsoft' in f.read().lower():
                        self.is_wsl2 = True
            except:
                pass
        
        # Set platform flags
        self.is_windows = system == 'Windows'
        self.is_linux = system == 'Linux' and not self.is_wsl2
        
        # Set platform string
        if self.is_windows:
            self.platform = 'windows'
        elif self.is_wsl2:
            self.platform = 'wsl2'
        else:
            self.platform = 'linux'
    
    def _detect_ide(self):
        """Detect which IDE is being used"""
        self.ide_option = os.getenv('IDE_OPTION', 'vscode').lower()
        
        # Validate IDE option
        valid_ides = ['vscode', 'kaggle', 'jupyter_notebook']
        if self.ide_option not in valid_ides:
            print(f"Warning: Unknown IDE '{self.ide_option}', defaulting to 'vscode'")
            self.ide_option = 'vscode'
    
    def _detect_environment(self):
        """Detect if we're at home or office based on environment variables"""
        self.home_dir_env = os.getenv('GBL_HOME_DIR')
        self.office_dir_env = os.getenv('GBL_OFFICE_DIR')
        
        # Will be set in _setup_base_directories
        self.location = None

    def get_conda_env(self, env_name='mapping_env'):
        """
        Get the conda environment path for current platform
        
        Args:
            env_name (str): Name of the conda environment (default: 'mapping_env')
            
        Returns:
            Path or None: Path to the conda environment if found
        """
        # Define potential conda installation locations based on platform
        if self.is_windows:
            conda_paths = [
                Path.home() / "AppData/Local/anaconda3",
                Path.home() / "AppData/Local/miniconda3",
                Path.home() / "miniconda3",
                Path.home() / "anaconda3",
                Path("C:/ProgramData/Anaconda3"),
                Path("C:/ProgramData/Miniconda3"),
            ]
        else:  # WSL2 or Linux
            conda_paths = [
                Path.home() / "miniconda3",
                Path.home() / "anaconda3",
                Path("/opt/conda"),
                Path("/usr/local/anaconda3"),
                Path("/usr/local/miniconda3"),
            ]
        
        # Look for conda installation
        for conda_base in conda_paths:
            if conda_base.exists():
                # Check for the specific environment
                env_path = conda_base / "envs" / env_name
                if env_path.exists():
                    return env_path
                
                # Also check if we're in the base environment
                if env_name == 'base' or env_name is None:
                    return conda_base
        
        # If not found, check if conda is in current Python path
        # This handles cases where script is already running in conda env
        current_python = Path(sys.executable)
        if 'conda' in str(current_python) or 'envs' in str(current_python):
            # Extract the environment path
            parts = current_python.parts
            if 'envs' in parts:
                envs_index = parts.index('envs')
                env_path = Path(*parts[:envs_index+2])
                return env_path
        
        return None
    
    def get_python_executable(self, env_name=None):
        """
        Get the Python executable path for a conda environment
        
        Args:
            env_name (str): Name of conda environment, None for current
            
        Returns:
            Path: Path to python executable
        """
        if env_name:
            conda_env = self.get_conda_env(env_name)
            if conda_env:
                if self.is_windows:
                    python_exe = conda_env / "python.exe"
                else:
                    python_exe = conda_env / "bin" / "python"
                
                if python_exe.exists():
                    return python_exe
        
        # Fallback to current Python
        return Path(sys.executable)
    
    def _load_application_settings(self):
        """
        Load application behaviour settings
        These can be overriden by environment variables or a settings file
        """

        # Default values for application settings
        self.DELETE_EXISTING_OUTPUT_FILES = False
        self.DISPLAY_VISUALIZATIONS = False
        self.SAVE_VISUALIZATIONS = False
        self.SAVE_DATA = True
        self.CSV_OUTPUT = True
        self.SQLITE_OUTPUT = False
        self.CONSOLIDATE_FILES = True

        # Allow env variable to override defaults
        # Convert string env vars to boolean
        if os.getenv('DELETE_EXISTING_OUTPUT_FILES') is not None:
            self.DELETE_EXISTING_OUTPUT_FILES = os.getenv('DELETE_EXISTING_OUTPUT_FILES', 'False').lower() in ['true', '1', 'yes']

        if os.getenv('DISPLAY_VISUALIZATIONS') is not None:
            self.DISPLAY_VISUALIZATIONS = os.getenv('DISPLAY_VISUALIZATIONS', 'False').lower() in ['true', '1', 'yes']

        if os.getenv('SAVE_VISUALIZATIONS') is not None:
            self.SAVE_VISUALIZATIONS = os.getenv('SAVE_VISUALIZATIONS', 'False').lower() in ['true', '1', 'yes']

        if os.getenv('SAVE_DATA') is not None:
            self.SAVE_DATA = os.getenv('SAVE_DATA', 'True').lower() in ['true', '1', 'yes']

        if os.getenv('CSV_OUTPUT') is not None:
            self.CSV_OUTPUT = os.getenv('CSV_OUTPUT', 'True').lower() in ['true', '1', 'yes']

        if os.getenv('SQLITE_OUTPUT') is not None:
            self.SQLITE_OUTPUT = os.getenv('SQLITE_OUTPUT', 'False').lower() in ['true', '1', 'yes']

        if os.getenv('CONSOLIDATE_FILES') is not None:
            self.CONSOLIDATE_FILES = os.getenv('CONSOLIDATE_FILES', 'True').lower() in ['true', '1', 'yes']

    def _convert_path_to_platform(self, path_str):
        """
        Convert a path string to work on the current platform
        Handles conversion between Windows and WSL2 paths
        """
        if not path_str:
            return None
        
        path_str = str(path_str)
        
        # If we're on Windows but path looks like WSL2/Linux
        if self.is_windows and path_str.startswith('/'):
            if path_str.startswith('/home/'):
                # Convert /home/username to Windows user directory
                parts = path_str.split('/')
                if len(parts) > 2:
                    # Extract the username from the WSL path
                    wsl_username = parts[2]
                    # Use Windows USERNAME env var
                    windows_username = os.environ.get('USERNAME', 'user')
                    # Replace the WSL home path with Windows path
                    remaining = '/'.join(parts[3:]) if len(parts) > 3 else ''
                    path_str = f"C:/Users/{windows_username}/{remaining}"
            elif path_str.startswith('/mnt/c'):
                # Convert /mnt/c to C:
                path_str = path_str.replace('/mnt/c', 'C:')
                path_str = path_str.replace('/', '\\')
        
        # If we're on WSL2 but path looks like Windows
        elif self.is_wsl2 and ':' in path_str:
            drive = path_str[0].lower()
            rest = path_str[2:].replace('\\', '/')
            path_str = f"/mnt/{drive}/{rest}"
        
        return Path(path_str)
    
    def _setup_base_directories(self):
        """Setup base directories based on IDE and environment"""
        
        if self.ide_option == 'vscode':
            # For vscode, determine if we're at home or office
            self._setup_vscode_directories()
            
        elif self.ide_option == 'kaggle':
            # Kaggle has fixed paths
            self.root_dir = Path('')
            self.base_output_dir = Path('/kaggle/working/output')
            self.base_input_dir = Path('/kaggle/input/')
            self.location = 'kaggle'
            
        elif self.ide_option == 'jupyter_notebook':
            # Jupyter setup depends on platform
            if self.is_windows:
                self.root_dir = Path('C:/Users/kaczanor/OneDrive - Enbridge Inc/Documents/Python/NEW_AESO_API')
            elif self.is_wsl2:
                self.root_dir = Path('/home/rob_kaz/python/projects/NEW_AESO_API')
            else:
                # Generic Linux
                self.root_dir = Path.home() / 'python/projects/NEW_AESO_API'
            
            self.base_output_dir = self.root_dir / 'output'
            self.base_input_dir = self.root_dir / 'object_mapping'
            self.location = 'jupyter'
    
    def _setup_vscode_directories(self):
        """Setup directories specifically for VSCode"""
        # Try environment variables first
        home_path = self._convert_path_to_platform(self.home_dir_env) if self.home_dir_env else None
        office_path = self._convert_path_to_platform(self.office_dir_env) if self.office_dir_env else None
        
        # Check which path exists
        if home_path and home_path.exists():
            self.root_dir = home_path
            self.location = 'home'
            print(f"Using HOME environment: {self.root_dir}")
        elif office_path and office_path.exists():
            self.root_dir = office_path
            self.location = 'office'
            print(f"Using OFFICE environment: {self.root_dir}")
        else:
            # Fallback: try to detect based on current platform
            if self.is_wsl2:
                default_path = Path('/home/rob_kaz/python/projects/NEW_AESO_API')
            elif self.is_windows:
                username = os.environ.get('USERNAME', 'user')
                default_path = Path(f'C:/Users/{username}/OneDrive - Enbridge Inc/Documents/Python/NEW_AESO_API')
            else:
                default_path = Path.home() / 'python/projects/NEW_AESO_API'
            
            if default_path.exists():
                self.root_dir = default_path
                self.location = 'detected'
                print(f"Using detected path: {self.root_dir}")
            else:
                # Last resort: use current working directory
                self.root_dir = Path.cwd()
                self.location = 'current_dir'
                print(f"Warning: Using current directory: {self.root_dir}")
        
        # Set input/output directories relative to root
        self.base_output_dir = self.root_dir / 'output'
        self.base_input_dir = self.root_dir / 'object_mapping'
    
    def normalize_path(self, path):
        """Normalize a file path to use forward slashes (backward compatibility)"""
        return os.path.normpath(path).replace('\\', '/')
    
    def resolve_path(self, path_str):
        """
        Resolve a path string to work on current platform
        Can handle both absolute and relative paths
        """
        if not path_str:
            return None
        
        path = Path(path_str)
        
        # If it's already an absolute path that exists, return it
        if path.is_absolute() and path.exists():
            return path
        
        # If it's relative, make it relative to root_dir
        if not path.is_absolute():
            resolved = self.root_dir / path
            if resolved.exists():
                return resolved
        
        # Try platform conversion for absolute paths
        converted = self._convert_path_to_platform(path_str)
        if converted and converted.exists():
            return converted
        
        # Return the best guess (might be for a file to be created)
        if path.is_absolute():
            return self._convert_path_to_platform(path_str)
        else:
            return self.root_dir / path
    
    def get_base_paths(self):
        """Return dictionary of base paths for easy access"""
        return {
            'root': self.root_dir,
            'input': self.base_input_dir,
            'output': self.base_output_dir,
            'src': self.root_dir / 'src',
            'docs': self.root_dir / 'docs',
            'test_code': self.root_dir / 'test_code',
        }

    def get_settings(self):
        """Return application settings"""
        return {
            'DELETE_EXISTING_OUTPUT_FILES': self.DELETE_EXISTING_OUTPUT_FILES,
            'DISPLAY_VISUALIZATIONS': self.DISPLAY_VISUALIZATIONS,
            'SAVE_VISUALIZATIONS': self.SAVE_VISUALIZATIONS,
            'SAVE_DATA': self.SAVE_DATA,
            'CSV_OUTPUT': self.CSV_OUTPUT,
            'SQLITE_OUTPUT': self.SQLITE_OUTPUT,
            'CONSOLIDATE_FILES': self.CONSOLIDATE_FILES
        }
    
    def update_settings(self, setting_name, value):
        """Update application settings"""
        if hasattr(self, setting_name):
            setattr(self, setting_name, value)
            print(f"Updated {setting_name}: {value}")
        else:
            print(f"Setting {setting_name} not found.")

    def print_config(self):
        """Print current configuration"""
        print("="*60)
        print("NEW_AESO_API - PLATFORM CONFIGURATION")
        print("="*60)
        print(f"Platform: {self.platform}")
        print(f"IDE: {self.ide_option}")
        print(f"Location: {self.location}")
        print(f"Python Version: {sys.version.split()[0]}")
        print("-"*60)
        print("DIRECTORIES:")
        print(f"Root: {self.root_dir}")
        print(f"Input: {self.base_input_dir}")
        print(f"Output: {self.base_output_dir}")
        print("="*60)
        print("APPLICATION SETTINGS")
        
        for key, value in self.get_settings().items():
            print(f"{key}: {value}")
        print("="*60)
    
    def get_global_vars_dict(self):
        """
        Return a dictionary of global variables for backward compatibility
        """
        return {
            'gbl_ide_option': self.ide_option,
            'gbl_home_dir': self.home_dir_env,
            'gbl_office_dir': self.office_dir_env,
            'gbl_root_dir': str(self.root_dir),
            'gbl_output_dir': str(self.base_output_dir),
            'gbl_input_dir': str(self.base_input_dir),
            
            # Application settings
            'DELETE_EXISTING_OUTPUT_FILES': self.DELETE_EXISTING_OUTPUT_FILES,
            'DISPLAY_VISUALIZATIONS': self.DISPLAY_VISUALIZATIONS,
            'SAVE_VISUALIZATIONS': self.SAVE_VISUALIZATIONS,
            'SAVE_DATA': self.SAVE_DATA,
            'CSV_OUTPUT': self.CSV_OUTPUT,
            'SQLITE_OUTPUT': self.SQLITE_OUTPUT,
            'CONSOLIDATE_FILES': self.CONSOLIDATE_FILES
        }

# Create singleton instance
platform_config = PlatformConfig()
