"""
Windows Hadoop/Winutils Fix for Spark
======================================
Downloads and configures winutils.exe for Spark on Windows
"""

import os
import sys
import logging
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

def setup_windows_hadoop():
    """
    Set up Hadoop environment for Windows
    Downloads winutils.exe if not present
    """
    if os.name != 'nt':
        return  # Not Windows, skip
    
    try:
        # Create Hadoop directory structure
        temp_base = os.environ.get('TEMP', 'C:\\Temp')
        hadoop_home = os.path.join(temp_base, 'hadoop')
        hadoop_bin = os.path.join(hadoop_home, 'bin')
        
        os.makedirs(hadoop_bin, exist_ok=True)
        
        winutils_path = os.path.join(hadoop_bin, 'winutils.exe')
        hadoop_dll_path = os.path.join(hadoop_bin, 'hadoop.dll')
        
        # Check if winutils.exe exists and is valid
        winutils_is_valid = False
        if os.path.exists(winutils_path):
            file_size = os.path.getsize(winutils_path)
            if file_size > 100000:  # Valid winutils should be > 100KB
                winutils_is_valid = True
                logger.info(f"✅ Valid winutils.exe found ({file_size} bytes)")
            else:
                logger.warning(f"⚠️ Invalid winutils.exe found ({file_size} bytes), removing...")
                try:
                    os.remove(winutils_path)
                except:
                    pass
        
        # Download winutils if needed
        if not winutils_is_valid:
            logger.info("📥 Downloading winutils.exe for Hadoop 3.3.x...")
            
            # Try multiple sources
            winutils_urls = [
                "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.0/bin/winutils.exe",
                "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe",
                "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe"
            ]
            
            hadoop_dll_urls = [
                "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.0/bin/hadoop.dll",
                "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/hadoop.dll",
            ]
            
            download_success = False
            for url in winutils_urls:
                try:
                    logger.info(f"  Trying: {url}")
                    urllib.request.urlretrieve(url, winutils_path)
                    
                    # Verify download
                    if os.path.exists(winutils_path) and os.path.getsize(winutils_path) > 100000:
                        logger.info(f"✅ Successfully downloaded winutils.exe ({os.path.getsize(winutils_path)} bytes)")
                        download_success = True
                        break
                    else:
                        logger.warning(f"  Download was too small, trying next source...")
                        try:
                            os.remove(winutils_path)
                        except:
                            pass
                except Exception as e:
                    logger.warning(f"  Failed: {e}")
                    continue
            
            # Try to download hadoop.dll as well
            if download_success:
                for url in hadoop_dll_urls:
                    try:
                        urllib.request.urlretrieve(url, hadoop_dll_path)
                        if os.path.exists(hadoop_dll_path) and os.path.getsize(hadoop_dll_path) > 10000:
                            logger.info(f"✅ Downloaded hadoop.dll ({os.path.getsize(hadoop_dll_path)} bytes)")
                            break
                    except:
                        continue
            
            if not download_success:
                logger.error("❌ Could not download valid winutils.exe from any source")
                logger.info("💡 Will configure Spark to work WITHOUT winutils (limited functionality)")
                # Don't create empty file - just skip it
        
        # Set environment variables
        os.environ['HADOOP_HOME'] = hadoop_home
        os.environ['hadoop.home.dir'] = hadoop_home
        os.environ['HADOOP_USER_NAME'] = 'hadoop'
        
        # Add to PATH
        hadoop_bin_normalized = os.path.normpath(hadoop_bin)
        if hadoop_bin_normalized not in os.environ.get('PATH', ''):
            os.environ['PATH'] = hadoop_bin_normalized + os.pathsep + os.environ.get('PATH', '')
        
        logger.info(f"✅ HADOOP_HOME set to: {hadoop_home}")
        logger.info("✅ Windows Hadoop environment configured")
        
        return hadoop_home
        
    except Exception as e:
        logger.error(f"❌ Failed to setup Windows Hadoop: {e}")
        logger.warning("⚠️ Continuing without winutils - some operations may fail")
        return None


def get_windows_spark_configs():
    """
    Get additional Spark configurations for Windows
    These help minimize the need for winutils.exe
    """
    if os.name != 'nt':
        return {}
    
    return {
        # Bypass most Hadoop filesystem checks
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored": "true",
        
        # Disable speculative execution
        "spark.speculation": "false",
        
        # Use local directories (with proper Windows paths)
        "spark.sql.warehouse.dir": "file:///C:/tmp/spark-warehouse",
        "spark.local.dir": "C:/tmp/spark-temp",
        
        # Network settings
        "spark.driver.host": "localhost",
        "spark.driver.bindAddress": "127.0.0.1",
        
        # Reduce file I/O operations
        "spark.sql.files.ignoreCorruptFiles": "true",
        "spark.sql.streaming.schemaInference": "false",
        
        # Memory settings to avoid disk spill issues
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g",
        
        # Disable problematic features on Windows
        "spark.ui.reverseProxy": "false",
        "spark.shuffle.service.enabled": "false",
        
        # CRITICAL: Bypass permission checks that require winutils
        "spark.hadoop.fs.permissions.umask-mode": "000",
        "spark.hadoop.dfs.permissions.enabled": "false",
        
        # Use in-memory catalog instead of file-based (avoids winutils mkdir issues)
        "spark.sql.catalogImplementation": "in-memory",
        
        # Disable checkpoint that requires filesystem operations
        "spark.streaming.receiver.writeAheadLog.enable": "false",
    }


if __name__ == "__main__":
    # Test the setup
    logging.basicConfig(level=logging.INFO)
    setup_windows_hadoop()
    print("\nWindows Spark configurations:")
    for key, value in get_windows_spark_configs().items():
        print(f"  {key} = {value}")
