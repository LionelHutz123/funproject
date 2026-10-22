# NBA Data Scraping System

A comprehensive, database-driven system for scraping, storing, and analyzing NBA basketball data.

## 🚀 Quick Start with Google Colab

### Option 1: Run from GitHub (Recommended)

1. **Open Google Colab**: [Google Colab](https://colab.research.google.com/)
2. **Create new notebook**
3. **Run this code**:

```python
# Clone the repository
!git clone https://github.com/yourusername/nba-data-scraping.git
%cd nba-data-scraping

# Install dependencies
!pip install -r requirements.txt
!playwright install firefox

# Setup and run
!python colab_quick_start.py
!python colab_scraper.py
```

### Option 2: Upload Files Manually

1. **Upload files** to Colab
2. **Run setup**:
```python
!pip install beautifulsoup4 pandas playwright sqlalchemy loguru pydantic aiohttp asyncio-throttle lxml requests psutil
!playwright install firefox
!python colab_quick_start.py
```

## 📁 Repository Structure

```
nba-data-scraping/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── config.py                   # Configuration settings
├── models.py                   # Database models
├── enhanced_scraper.py         # Advanced scraper
├── cloud_setup.py              # Cloud environment setup
├── colab_quick_start.py        # Colab-specific setup
├── colab_scraper.py            # Colab-optimized scraper
├── main.py                     # Command-line interface
├── database_manager.py         # Database queries
├── prediction_data_analysis.py # Data analysis tools
├── cloud_deployment_guide.md   # Cloud deployment guide
└── requirements_cloud.txt       # Cloud-optimized requirements
```

## 🛠️ Installation

### Local Installation
```bash
git clone https://github.com/yourusername/nba-data-scraping.git
cd nba-data-scraping
pip install -r requirements.txt
playwright install firefox
```

### Cloud Installation
```bash
# For AWS/DigitalOcean
python cloud_setup.py
./start_cloud_scraping.sh
```

## 📊 Usage

### Basic Commands
```bash
# Setup database
python main.py setup

# Process existing files
python main.py process

# Scrape recent data
python main.py scrape-recent

# View statistics
python main.py stats

# Get team standings
python main.py standings --season 2024

# Get player stats
python main.py player --player "LeBron James" --season 2024
```

### Advanced Analysis
```bash
# Comprehensive data analysis
python prediction_data_analysis.py

# Run complete scraping
python run_enhanced_scraping.py
```

## ☁️ Cloud Deployment

### Google Colab (FREE)
- Perfect for testing and development
- 12GB RAM, GPU access
- No setup required

### DigitalOcean ($5/month)
- Light scraping and analysis
- 1GB RAM, 1 vCPU
- Simple setup

### AWS EC2 (~$30/month)
- Production scraping
- 4GB RAM, 2 vCPU
- Full-scale analysis

## 📈 Features

- **Database-driven architecture** with SQLite
- **Advanced scraping** with rate limiting and error handling
- **Comprehensive data collection** from multiple sources
- **Prediction-ready features** for machine learning
- **Cloud-optimized** for cost-effective deployment
- **Real-time monitoring** and logging

## 🔧 Configuration

Edit `config.py` to customize:
- Database settings
- Scraping parameters
- Season ranges
- Cloud settings

## 📊 Data Sources

- **Game data**: Box scores, team stats, player stats
- **Team data**: Rosters, advanced stats, efficiency metrics
- **Player data**: Profiles, career stats, biographical information
- **Historical data**: Season standings, leaders, trends

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🆘 Support

For issues and questions:
1. Check the logs in the `logs/` directory
2. Review the configuration in `config.py`
3. Ensure all dependencies are installed
4. Verify database tables are created

## 🚀 Quick Links

- [Google Colab](https://colab.research.google.com/)
- [GitHub Repository](https://github.com/yourusername/nba-data-scraping)
- [Cloud Deployment Guide](cloud_deployment_guide.md)

### Certificate trust update (October 2026)

Use `certifi>=2026.10.19` and `pip>=26.2.2` in every Python environment.
Existing virtual environments and packaged applications are not updated by a
Git pull. Bootstrap pip using the hash-verified wheel in the
[Python security notice](https://discuss.python.org/t/information-for-python-users-regarding-emudhra-emsign-ca-certificate-security-incident/109576),
then upgrade certifi. If using uv, require version 0.13.4 or newer.
Rebuild containers with `--pull --no-cache` and current OS CA packages; restart
long-running clients through the project's deployment procedure. Do not disable
TLS verification or use pip `--trusted-host` to work around certificate errors.
