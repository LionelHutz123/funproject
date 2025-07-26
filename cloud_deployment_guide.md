# Cloud Deployment Guide for NBA Data Scraping

## 🚀 **Recommended Cloud Solutions**

### **1. Google Colab (FREE - Best Starting Point)**

**Setup**:
1. Go to [Google Colab](https://colab.research.google.com/)
2. Create new notebook
3. Upload your scripts to Google Drive
4. Run with GPU acceleration

**Cost**: $0 (free tier)
**Best for**: Testing and development

```python
# Colab setup code
!pip install beautifulsoup4 pandas playwright sqlalchemy loguru pydantic
!playwright install firefox
!git clone <your-repo-url>
%cd Gamebox
!python cloud_setup.py
!python cloud_scraper.py
```

### **2. AWS EC2 (Pay-as-you-go)**

**Recommended Instance**:
- **t3.medium**: $0.0416/hour (~$30/month)
- 2 vCPU, 4GB RAM, EBS storage

**Setup Script**:
```bash
# Launch Ubuntu 20.04 instance
sudo apt update
sudo apt install -y python3-pip git screen
git clone <your-repo>
cd Gamebox
python3 cloud_setup.py
screen -S scraping
python3 cloud_scraper.py
# Ctrl+A+D to detach
```

### **3. DigitalOcean Droplets (Simple & Affordable)**

**Recommended Plan**:
- **Basic Droplet**: $5/month
- 1 vCPU, 1GB RAM, 25GB SSD

**Setup**:
```bash
# Create droplet with Ubuntu
ssh root@your-droplet-ip
apt update && apt install -y python3-pip git
git clone <your-repo>
cd Gamebox
python3 cloud_setup.py
nohup python3 cloud_scraper.py > scraping.log 2>&1 &
```

### **4. Google Cloud Platform (Free Tier)**

**Free Tier Includes**:
- 1 f1-micro instance (1 vCPU, 0.6GB RAM)
- 30GB storage
- Perfect for light scraping

## 💰 **Cost Comparison**

| Service | Monthly Cost | RAM | CPU | Best For |
|---------|-------------|-----|-----|----------|
| **Google Colab** | $0 | 12GB | GPU | Testing |
| **AWS t3.medium** | ~$30 | 4GB | 2 vCPU | Production |
| **DigitalOcean $5** | $5 | 1GB | 1 vCPU | Light scraping |
| **Google Cloud f1-micro** | $0 | 0.6GB | 1 vCPU | Basic tasks |

## 🛠️ **Cloud-Optimized Setup**

### **Step 1: Prepare Your Code**

1. **Clone your repository** to the cloud instance
2. **Run cloud setup**:
```bash
python3 cloud_setup.py
```

3. **Start scraping**:
```bash
./start_cloud_scraping.sh
```

### **Step 2: Optimize for Your Cloud Environment**

The `cloud_setup.py` script automatically:
- Detects your cloud environment
- Optimizes settings for available resources
- Creates lightweight scraping scripts
- Sets up resource monitoring

### **Step 3: Monitor and Manage**

```bash
# Check resource usage
python3 cloud_monitor.py

# View logs
tail -f logs/cloud_scraping.log

# Check database stats
python3 main.py stats
```

## 📊 **Resource Requirements**

### **Minimum Requirements**
- **RAM**: 1GB (2GB recommended)
- **Storage**: 10GB (20GB recommended)
- **CPU**: 1 vCPU (2 vCPU recommended)
- **Network**: Stable internet connection

### **Recommended for Production**
- **RAM**: 4GB
- **Storage**: 50GB
- **CPU**: 2 vCPU
- **Network**: High-speed connection

## 🔧 **Cloud-Specific Optimizations**

### **Memory Management**
```python
# Cloud-optimized batch processing
BATCH_SIZE = 10  # Smaller batches for limited RAM
MEMORY_LIMIT = 0.8  # Use 80% of available memory
```

### **Network Optimization**
```python
# Reduced concurrent requests for cloud
MAX_CONCURRENT = 2  # Lower than local
REQUEST_DELAY = 2.0  # Higher delay to be respectful
```

### **Storage Management**
```python
# Compress data for cloud storage
import gzip
import json

def save_compressed_data(data, filename):
    with gzip.open(f"{filename}.gz", 'wt') as f:
        json.dump(data, f)
```

## 🚀 **Quick Start Guide**

### **Option 1: Google Colab (Free)**
1. Open [Google Colab](https://colab.research.google.com/)
2. Create new notebook
3. Run setup commands:
```python
!pip install -r requirements.txt
!git clone <your-repo>
%cd Gamebox
!python cloud_setup.py
!python cloud_scraper.py
```

### **Option 2: AWS EC2 ($30/month)**
1. Launch Ubuntu instance (t3.medium)
2. Connect via SSH
3. Run setup:
```bash
sudo apt update
sudo apt install -y python3-pip git
git clone <your-repo>
cd Gamebox
python3 cloud_setup.py
screen -S scraping
python3 cloud_scraper.py
```

### **Option 3: DigitalOcean ($5/month)**
1. Create Ubuntu droplet
2. Connect via SSH
3. Run setup:
```bash
apt update && apt install -y python3-pip git
git clone <your-repo>
cd Gamebox
python3 cloud_setup.py
nohup python3 cloud_scraper.py > scraping.log 2>&1 &
```

## 📈 **Performance Tips**

### **For Limited Resources**
1. **Reduce batch size**: Process fewer games at once
2. **Increase delays**: Be more respectful to servers
3. **Monitor memory**: Stop if memory usage > 80%
4. **Use compression**: Save disk space

### **For Better Performance**
1. **Use larger instances**: More RAM and CPU
2. **Enable GPU**: For data processing (Colab)
3. **Optimize database**: Use SQLite with proper indexing
4. **Parallel processing**: Use multiple processes

## 🔍 **Monitoring and Maintenance**

### **Resource Monitoring**
```bash
# Check system resources
htop
df -h
free -h

# Monitor scraping progress
tail -f logs/cloud_scraping.log
```

### **Database Management**
```bash
# Check database size
ls -lh basketball_data.db

# Backup database
cp basketball_data.db backup_$(date +%Y%m%d).db

# Optimize database
sqlite3 basketball_data.db "VACUUM;"
```

## 💡 **Cost Optimization Tips**

### **1. Use Spot Instances (AWS)**
- Save 60-90% on compute costs
- Good for non-critical workloads

### **2. Schedule Scraping**
- Run during off-peak hours
- Use cron jobs for automation

### **3. Data Compression**
- Compress JSON files
- Use efficient database storage

### **4. Clean Up Regularly**
- Remove old log files
- Archive old data
- Optimize database

## 🛡️ **Security Considerations**

### **1. Secure Your Instance**
```bash
# Update regularly
sudo apt update && sudo apt upgrade

# Configure firewall
sudo ufw allow ssh
sudo ufw enable
```

### **2. Protect Your Data**
```bash
# Encrypt sensitive data
# Use environment variables for API keys
# Regular backups
```

## 📞 **Support and Troubleshooting**

### **Common Issues**

**1. Memory Issues**
```bash
# Check memory usage
free -h
# Kill memory-intensive processes
pkill -f python
```

**2. Disk Space Issues**
```bash
# Check disk usage
df -h
# Clean up old files
rm -rf logs/*.log.old
```

**3. Network Issues**
```bash
# Test connectivity
ping basketball-reference.com
# Check DNS
nslookup basketball-reference.com
```

### **Getting Help**
1. Check logs in `logs/` directory
2. Monitor resource usage with `cloud_monitor.py`
3. Use `python3 main.py stats` to check data status

## 🎯 **Recommended Workflow**

### **For Testing (Google Colab)**
1. Test your scripts in Colab
2. Validate data quality
3. Optimize performance
4. Scale to production

### **For Production (AWS/DigitalOcean)**
1. Set up cloud instance
2. Run `cloud_setup.py`
3. Start scraping with `start_cloud_scraping.sh`
4. Monitor with `cloud_monitor.py`
5. Check results with `main.py stats`

This cloud deployment guide provides cost-effective solutions for running your NBA data scraping system efficiently in the cloud! 