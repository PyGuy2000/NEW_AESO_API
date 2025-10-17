"""
Core package for platform-aware configuration and file handling
"""

from .platform_config import platform_config
from .file_handler import file_handler

__all__ = ['platform_config', 'file_handler']
