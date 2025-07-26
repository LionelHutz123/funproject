#!/usr/bin/env python3
"""
Cloud-optimized setup for NBA data scraping
Optimized for low-cost cloud environments
"""

import os
import sys
import subprocess
import json
from pathlib import Path
from loguru import logger

class CloudOptimizer:
    def __init__(self):
        self.config = {
            'max_concurrent_requests': 2,  # Reduced for cloud
            'request_delay': 2.0,  # Increased delay
            'batch_size': 10,  # Process in smaller batches
            'memory_limit': 0.8,  # Use 80% of available memory
            'disk_limit': 0.7,  # Use 70% of available disk
        }
    
    def detect_cloud_environment(self):
        """Detect if running in cloud environment"""
        cloud_indicators = {
            'aws': ['AWS_', 'EC2_'],
            'gcp': ['GOOGLE_CLOUD_', 'GCP_'],
            'azure': ['AZURE_', 'MSFT_'],
            'digitalocean': ['DIGITALOCEAN_', 'DO_']
        }
        
        for cloud, prefixes in cloud_indicators.items():
            for prefix in prefixes:
                if any(env.startswith(prefix) for env in os.environ.keys()):
                    logger.info(f"Detected {cloud.upper()} environment")
                    return cloud
        
        return 'local'
    
    def optimize_for_cloud(self, cloud_type):
        """Optimize configuration for specific cloud environment"""
        if cloud_type == 'aws':
            self.config.update({
                'max_concurrent_requests': 3,
                'request_delay': 1.5,
                'batch_size': 15,
                'memory_limit': 0.7,  # Conservative for AWS
            })
        elif cloud_type == 'gcp':
            self.config.update({
                'max_concurrent_requests': 4,
                'request_delay': 1.0,
                'batch_size': 20,
                'memory_limit': 0.8,
            })
        elif cloud_type == 'digitalocean':
            self.config.update({
                'max_concurrent_requests': 2,
                'request_delay': 2.0,
                'batch_size': 10,
                'memory_limit': 0.6,  # Conservative for DO
            })
        
        logger.info(f"Optimized for {cloud_type} environment")
        return self.config
    
    def check_system_resources(self):
        """Check available system resources"""
        import psutil
        
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        logger.info(f"System Resources:")
        logger.info(f"  Memory: {memory.total / (1024**3):.1f}GB total, {memory.available / (1024**3):.1f}GB available")
        logger.info(f"  Disk: {disk.total / (1024**3):.1f}GB total, {disk.free / (1024**3):.1f}GB available")
        logger.info(f"  CPU Cores: {psutil.cpu_count()}")
        
        return {
            'memory_gb': memory.total / (1024**3),
            'disk_gb': disk.total / (1024**3),
            'cpu_cores': psutil.cpu_count()
        }

def create_cloud_config():
    """Create cloud-optimized configuration"""
    optimizer = CloudOptimizer()
    cloud_type = optimizer.detect_cloud_environment()
    config = optimizer.optimize_for_cloud(cloud_type)
    resources = optimizer.check_system_resources()
    
    # Create cloud-optimized config
    cloud_config = {
        'cloud_environment': cloud_type,
        'system_resources': resources,
        'optimization_settings': config,
        'scraping_settings': {
            'max_retries': 3,
            'timeout': 30,
            'headless': True,
            'rate_limit': config['request_delay'],
            'batch_processing': True,
            'batch_size': config['batch_size'],
            'memory_monitoring': True,
            'disk_monitoring': True,
        }
    }
    
    # Save configuration
    with open('cloud_config.json', 'w') as f:
        json.dump(cloud_config, f, indent=2)
    
    logger.info("Cloud configuration created: cloud_config.json")
    return cloud_config

def install_cloud_dependencies():
    """Install dependencies optimized for cloud"""
    logger.info("Installing cloud-optimized dependencies...")
    
    # Install system dependencies
    system_packages = [
        'build-essential',
        'python3-dev',
        'libffi-dev',
        'libssl-dev',
        'libxml2-dev',
        'libxslt1-dev',
        'zlib1g-dev'
    ]
    
    try:
        subprocess.run(['sudo', 'apt-get', 'update'], check=True)
        for package in system_packages:
            subprocess.run(['sudo', 'apt-get', 'install', '-y', package], check=True)
        logger.info("System dependencies installed")
    except subprocess.CalledProcessError as e:
        logger.warning(f"Could not install system packages: {e}")
    
    # Install Python dependencies
    try:
        subprocess.run([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'], check=True)
        subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'], check=True)
        logger.info("Python dependencies installed")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error installing Python dependencies: {e}")

def create_cloud_scripts():
    """Create cloud-optimized scripts"""
    
    # Create lightweight scraper
    lightweight_scraper = '''#!/usr/bin/env python3
"""
Lightweight scraper optimized for cloud environments
"""

import asyncio
import sys
from loguru import logger
from enhanced_scraper import EnhancedBasketballScraper

async def cloud_scrape_recent_seasons():
    """Scrape recent seasons with cloud optimizations"""
    logger.info("Starting cloud-optimized scraping...")
    
    # Load cloud configuration
    import json
    with open('cloud_config.json', 'r') as f:
        cloud_config = json.load(f)
    
    # Apply cloud optimizations
    settings = cloud_config['scraping_settings']
    
    async with EnhancedBasketballScraper() as scraper:
        # Override settings for cloud
        scraper.throttler = Throttler(rate_limit=1, period=settings['rate_limit'])
        
        # Scrape only current and previous season for efficiency
        current_year = 2024
        seasons = [current_year - 1, current_year]
        
        for season in seasons:
            logger.info(f"Scraping season {season}...")
            game_urls = await scraper.scrape_season_schedule(season)
            
            # Process in batches
            batch_size = settings['batch_size']
            for i in range(0, len(game_urls), batch_size):
                batch = game_urls[i:i + batch_size]
                
                for url in batch:
                    game_data = await scraper.scrape_comprehensive_game_data(url)
                    if game_data:
                        await save_comprehensive_game_data(game_data)
                
                logger.info(f"Processed batch {i//batch_size + 1}/{(len(game_urls) + batch_size - 1)//batch_size}")
                
                # Memory management
                if i % (batch_size * 5) == 0:
                    import gc
                    gc.collect()
        
        logger.info("Cloud scraping completed!")

if __name__ == "__main__":
    asyncio.run(cloud_scrape_recent_seasons())
'''
    
    with open('cloud_scraper.py', 'w') as f:
        f.write(lightweight_scraper)
    
    # Create monitoring script
    monitoring_script = '''#!/usr/bin/env python3
"""
Resource monitoring for cloud environments
"""

import psutil
import time
import json
from loguru import logger

def monitor_resources():
    """Monitor system resources during scraping"""
    while True:
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        cpu = psutil.cpu_percent()
        
        status = {
            'timestamp': time.time(),
            'memory_percent': memory.percent,
            'disk_percent': disk.percent,
            'cpu_percent': cpu,
            'memory_available_gb': memory.available / (1024**3),
            'disk_free_gb': disk.free / (1024**3)
        }
        
        # Log if resources are getting low
        if memory.percent > 80 or disk.percent > 80 or cpu > 90:
            logger.warning(f"High resource usage: Memory {memory.percent}%, Disk {disk.percent}%, CPU {cpu}%")
        
        # Save status
        with open('resource_status.json', 'w') as f:
            json.dump(status, f)
        
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    monitor_resources()
'''
    
    with open('cloud_monitor.py', 'w') as f:
        f.write(monitoring_script)
    
    logger.info("Cloud-optimized scripts created")

def main():
    """Main cloud setup function"""
    logger.info("Setting up cloud-optimized NBA data scraping system...")
    
    # Step 1: Detect and optimize for cloud environment
    logger.info("Step 1: Detecting cloud environment...")
    cloud_config = create_cloud_config()
    
    # Step 2: Install dependencies
    logger.info("Step 2: Installing dependencies...")
    install_cloud_dependencies()
    
    # Step 3: Create cloud-optimized scripts
    logger.info("Step 3: Creating cloud-optimized scripts...")
    create_cloud_scripts()
    
    # Step 4: Create startup script
    startup_script = '''#!/bin/bash
# Cloud startup script for NBA data scraping

echo "Starting cloud-optimized NBA data scraping..."

# Check if we're in a cloud environment
if [ -f "cloud_config.json" ]; then
    echo "Cloud configuration found"
else
    echo "Creating cloud configuration..."
    python3 cloud_setup.py
fi

# Start resource monitoring in background
python3 cloud_monitor.py &

# Run the cloud scraper
python3 cloud_scraper.py

echo "Cloud scraping completed!"
'''
    
    with open('start_cloud_scraping.sh', 'w') as f:
        f.write(startup_script)
    
    # Make executable
    os.chmod('start_cloud_scraping.sh', 0o755)
    
    logger.info("Cloud setup completed!")
    logger.info("To start scraping, run: ./start_cloud_scraping.sh")

if __name__ == "__main__":
    main() 