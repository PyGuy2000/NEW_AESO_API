"""
Test script to verify platform configuration is working correctly
Run this from both Windows and WSL2 to verify paths are detected properly
"""

from core import platform_config, file_handler

def test_platform_config():
    """Test platform configuration"""
    print("\n" + "="*70)
    print("TESTING PLATFORM CONFIGURATION")
    print("="*70)
    
    # Test 1: Platform detection
    print(f"\n1. Platform Detection:")
    print(f"   - Platform: {platform_config.platform}")
    print(f"   - Is Windows: {platform_config.is_windows}")
    print(f"   - Is WSL2: {platform_config.is_wsl2}")
    print(f"   - Is Linux: {platform_config.is_linux}")
    
    # Test 2: Base paths
    print(f"\n2. Base Paths:")
    paths = platform_config.get_base_paths()
    for name, path in paths.items():
        exists = "✓" if path.exists() else "✗"
        print(f"   - {name}: {path} [{exists}]")
    
    # Test 3: Settings
    print(f"\n3. Application Settings:")
    settings = platform_config.get_settings()
    for key, value in settings.items():
        print(f"   - {key}: {value}")
    
    # Test 4: File handler
    print(f"\n4. File Handler Tests:")
    
    # Test reading an existing file
    try:
        mapping_files = file_handler.get_file_list('object_mapping', '*.csv')
        print(f"   - Found {len(mapping_files)} CSV files in object_mapping:")
        for f in mapping_files:
            print(f"     • {f.name}")
    except Exception as e:
        print(f"   - Error reading mapping files: {e}")
    
    # Test path resolution
    test_path = "output/test_file.csv"
    resolved = platform_config.resolve_path(test_path)
    print(f"\n5. Path Resolution Test:")
    print(f"   - Input: {test_path}")
    print(f"   - Resolved: {resolved}")
    
    print("\n" + "="*70)
    print("TEST COMPLETE")
    print("="*70 + "\n")

if __name__ == "__main__":
    test_platform_config()
