"""
Setup HADOOP_HOME for Windows
Downloads winutils.exe required for Spark on Windows
"""
import os
import sys
import urllib.request
from pathlib import Path

def setup_hadoop_home():
    """Setup HADOOP_HOME directory with winutils.exe for Windows"""
    
    print("=" * 60)
    print("Setting up HADOOP_HOME for Windows")
    print("=" * 60)
    
    # Determine HADOOP_HOME location
    home = Path.home()
    hadoop_home = home / ".hadoop_home"
    hadoop_bin = hadoop_home / "bin"
    
    # Create directories
    hadoop_home.mkdir(exist_ok=True)
    hadoop_bin.mkdir(exist_ok=True)
    
    print(f"\n✓ HADOOP_HOME directory: {hadoop_home}")
    
    # Check if winutils.exe already exists
    winutils_path = hadoop_bin / "winutils.exe"
    
    if winutils_path.exists():
        print(f"✓ winutils.exe already exists: {winutils_path}")
    else:
        print(f"\nDownloading winutils.exe...")
        
        # Hadoop 3.2.0 winutils for Windows
        winutils_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe"
        
        try:
            print(f"  URL: {winutils_url}")
            urllib.request.urlretrieve(winutils_url, winutils_path)
            print(f"✓ Downloaded winutils.exe to: {winutils_path}")
        except Exception as e:
            print(f"✗ Failed to download winutils.exe: {e}")
            print("\nAlternative: Download manually from:")
            print("  https://github.com/cdarlint/winutils/tree/master/hadoop-3.2.0/bin")
            print(f"  and place winutils.exe in: {hadoop_bin}")
            return False
    
    # Download hadoop.dll (required for NativeIO on Windows)
    hadoop_dll_path = hadoop_bin / "hadoop.dll"
    if hadoop_dll_path.exists():
        print(f"✓ hadoop.dll already exists: {hadoop_dll_path}")
    else:
        print(f"\nDownloading hadoop.dll...")
        
        hadoop_dll_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/hadoop.dll"
        
        try:
            print(f"  URL: {hadoop_dll_url}")
            urllib.request.urlretrieve(hadoop_dll_url, hadoop_dll_path)
            print(f"✓ Downloaded hadoop.dll to: {hadoop_dll_path}")
        except Exception as e:
            print(f"✗ Failed to download hadoop.dll: {e}")
            print(f"  This may cause NativeIO errors - download manually from:")
            print(f"  https://github.com/cdarlint/winutils/tree/master/hadoop-3.2.0/bin")
            print(f"  and place hadoop.dll in: {hadoop_bin}")
            # Don't fail setup just for hadoop.dll
    
    # Set environment variable
    os.environ['HADOOP_HOME'] = str(hadoop_home)
    
    print(f"\n✓ HADOOP_HOME set to: {hadoop_home}")
    print("\nTo make this permanent, add to your environment variables:")
    print(f"  Variable: HADOOP_HOME")
    print(f"  Value: {hadoop_home}")
    
    print("\n" + "=" * 60)
    print("✓ HADOOP_HOME setup complete!")
    print("=" * 60)
    
    return True


if __name__ == "__main__":
    success = setup_hadoop_home()
    sys.exit(0 if success else 1)
