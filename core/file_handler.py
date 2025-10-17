"""
File Handler Module for NEW_AESO_API
Platform-agnostic file handler for all file operations
"""

from pathlib import Path
import pandas as pd
import json
from .platform_config import platform_config

class FileHandler:
    """
    Platform-agnostic file handler for all file operations
    """

    def __init__(self):
        self.platform_config = platform_config

    def read_csv(self, file_path, **kwargs):
        """
        Read CSV file with platform-aware path resolution
        
        Args:
            file_path: Path to CSV file (can be relative or absolute)
            **kwargs: Additional arguments to pass to pd.read_csv
            
        Returns:
            DataFrame: Loaded CSV data
        """
        resolved_path = self.platform_config.resolve_path(file_path)
        if not resolved_path.exists():
            raise FileNotFoundError(f"File not found: {resolved_path}")
        return pd.read_csv(resolved_path, **kwargs)

    def write_csv(self, df, file_path, **kwargs):
        """
        Write CSV file with platform-aware path resolution
        
        Args:
            df: DataFrame to write
            file_path: Path where to save CSV (can be relative or absolute)
            **kwargs: Additional arguments to pass to df.to_csv
            
        Returns:
            Path: Resolved path where file was saved
        """
        resolved_path = self.platform_config.resolve_path(file_path)
        resolved_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(resolved_path, **kwargs)
        print(f"Saved CSV to: {resolved_path}")
        return resolved_path

    def read_json(self, file_path):
        """
        Read JSON file with platform-aware path resolution
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            dict: Loaded JSON data
        """
        resolved_path = self.platform_config.resolve_path(file_path)
        if not resolved_path.exists():
            raise FileNotFoundError(f"File not found: {resolved_path}")
        with open(resolved_path, 'r') as f:
            return json.load(f)

    def write_json(self, data, file_path, **kwargs):
        """
        Write JSON file with platform-aware path resolution
        
        Args:
            data: Data to write (dict or list)
            file_path: Path where to save JSON
            **kwargs: Additional arguments for json.dump (e.g., indent)
            
        Returns:
            Path: Resolved path where file was saved
        """
        resolved_path = self.platform_config.resolve_path(file_path)
        resolved_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Set default indent if not provided
        if 'indent' not in kwargs:
            kwargs['indent'] = 2
            
        with open(resolved_path, 'w') as f:
            json.dump(data, f, **kwargs)
        print(f"Saved JSON to: {resolved_path}")
        return resolved_path

    def get_file_list(self, directory, pattern="*"):
        """
        Get list of files in directory with pattern matching
        
        Args:
            directory: Directory to search
            pattern: Glob pattern for file matching (default: "*")
            
        Returns:
            list: List of Path objects matching the pattern
        """
        resolved_path = self.platform_config.resolve_path(directory)
        if not resolved_path.exists():
            raise FileNotFoundError(f"Directory not found: {resolved_path}")
        return list(resolved_path.glob(pattern))

    def ensure_directory(self, directory):
        """
        Ensure a directory exists, create if it doesn't
        
        Args:
            directory: Directory path to ensure exists
            
        Returns:
            Path: Resolved directory path
        """
        resolved_path = self.platform_config.resolve_path(directory)
        resolved_path.mkdir(parents=True, exist_ok=True)
        return resolved_path

    def file_exists(self, file_path):
        """
        Check if a file exists
        
        Args:
            file_path: Path to check
            
        Returns:
            bool: True if file exists, False otherwise
        """
        resolved_path = self.platform_config.resolve_path(file_path)
        return resolved_path.exists()

    def read_text(self, file_path, encoding='utf-8'):
        """
        Read text file with platform-aware path resolution
        
        Args:
            file_path: Path to text file
            encoding: Text encoding (default: 'utf-8')
            
        Returns:
            str: File contents
        """
        resolved_path = self.platform_config.resolve_path(file_path)
        if not resolved_path.exists():
            raise FileNotFoundError(f"File not found: {resolved_path}")
        return resolved_path.read_text(encoding=encoding)

    def write_text(self, content, file_path, encoding='utf-8'):
        """
        Write text file with platform-aware path resolution
        
        Args:
            content: Text content to write
            file_path: Path where to save text file
            encoding: Text encoding (default: 'utf-8')
            
        Returns:
            Path: Resolved path where file was saved
        """
        resolved_path = self.platform_config.resolve_path(file_path)
        resolved_path.parent.mkdir(parents=True, exist_ok=True)
        resolved_path.write_text(content, encoding=encoding)
        print(f"Saved text file to: {resolved_path}")
        return resolved_path

    def delete_file(self, file_path):
        """
        Delete a file if it exists
        
        Args:
            file_path: Path to file to delete
            
        Returns:
            bool: True if file was deleted, False if it didn't exist
        """
        resolved_path = self.platform_config.resolve_path(file_path)
        if resolved_path.exists():
            resolved_path.unlink()
            print(f"Deleted file: {resolved_path}")
            return True
        return False

    def get_output_path(self, subfolder=None, filename=None):
        """
        Helper to construct output paths
        
        Args:
            subfolder: Optional subfolder within output directory
            filename: Optional filename
            
        Returns:
            Path: Constructed output path
        """
        base = self.platform_config.base_output_dir
        
        if subfolder:
            base = base / subfolder
            
        if filename:
            base = base / filename
            
        return base

    def get_input_path(self, subfolder=None, filename=None):
        """
        Helper to construct input paths
        
        Args:
            subfolder: Optional subfolder within input directory
            filename: Optional filename
            
        Returns:
            Path: Constructed input path
        """
        base = self.platform_config.base_input_dir
        
        if subfolder:
            base = base / subfolder
            
        if filename:
            base = base / filename
            
        return base

# Create singleton instance
file_handler = FileHandler()
