#!/usr/bin/env python3
"""
Upload NBA data scraping project to GitHub repository
"""

import os
import subprocess
import sys
from pathlib import Path

def check_git_installed():
    """Check if git is installed"""
    try:
        subprocess.run(['git', '--version'], check=True, capture_output=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def setup_git_repo():
    """Setup git repository and push to GitHub"""
    if not check_git_installed():
        print("❌ Git is not installed. Please install git first.")
        return False
    
    try:
        # Initialize git repository
        subprocess.run(['git', 'init'], check=True)
        print("✅ Git repository initialized")
        
        # Add all files
        subprocess.run(['git', 'add', '.'], check=True)
        print("✅ Files added to git")
        
        # Initial commit
        subprocess.run(['git', 'commit', '-m', 'Initial NBA data scraping system'], check=True)
        print("✅ Initial commit created")
        
        # Add remote (your specific repository)
        remote_url = "https://github.com/LionelHutz123/funproject.git"
        subprocess.run(['git', 'remote', 'add', 'origin', remote_url], check=True)
        print(f"✅ Remote added: {remote_url}")
        
        # Set main branch
        subprocess.run(['git', 'branch', '-M', 'main'], check=True)
        print("✅ Main branch set")
        
        # Push to GitHub
        subprocess.run(['git', 'push', '-u', 'origin', 'main'], check=True)
        print("✅ Code pushed to GitHub!")
        
        print(f"\n🎉 Your repository is now available at:")
        print(f"https://github.com/LionelHutz123/funproject")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Git error: {e}")
        print("Make sure you have access to the repository.")
        return False

def create_colab_notebook():
    """Create a Colab notebook for your specific repository"""
    notebook_content = '''{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "header"
   },
   "source": [
    "# NBA Data Scraping - From GitHub\n",
    "\n",
    "This notebook runs the NBA data scraping system from your GitHub repository."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "clone_repo"
   },
   "outputs": [],
   "source": [
    "# Clone your repository from GitHub\n",
    "!git clone https://github.com/LionelHutz123/funproject.git\n",
    "%cd funproject\n",
    "print(\"Repository cloned successfully!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "install_deps"
   },
   "outputs": [],
   "source": [
    "# Install dependencies\n",
    "!pip install -r requirements.txt\n",
    "print(\"Dependencies installed!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "install_browser"
   },
   "outputs": [],
   "source": [
    "# Install Playwright browser\n",
    "!playwright install firefox\n",
    "print(\"Browser installed!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "setup_and_run"
   },
   "outputs": [],
   "source": [
    "# Setup and run the scraper\n",
    "!python colab_quick_start.py\n",
    "!python colab_scraper.py\n",
    "print(\"Scraping completed!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "check_results"
   },
   "outputs": [],
   "source": [
    "# Check results\n",
    "!python main.py stats\n",
    "!ls -la data/\n",
    "print(\"\\nResults ready for download!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "download_results"
   },
   "outputs": [],
   "source": [
    "# Download results (optional)\n",
    "from google.colab import files\n",
    "import zipfile\n",
    "import os\n",
    "\n",
    "# Create zip file of results\n",
    "if os.path.exists('data'):\n",
    "    with zipfile.ZipFile('nba_data_results.zip', 'w') as zipf:\n",
    "        for root, dirs, files in os.walk('data'):\n",
    "            for file in files:\n",
    "                file_path = os.path.join(root, file)\n",
    "                zipf.write(file_path, os.path.relpath(file_path, '.'))\n",
    "    \n",
    "    # Download the zip file\n",
    "    files.download('nba_data_results.zip')\n",
    "    print(\"Results downloaded!\")"
   ]
  }
 ],
 "metadata": {
  "accelerator": "GPU",
  "colab": {
   "gpuType": "T4",
   "provenance": []
  },
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}'''
    
    with open('nba_scraping_colab.ipynb', 'w') as f:
        f.write(notebook_content)
    
    print("✅ Colab notebook created: nba_scraping_colab.ipynb")
    print("Upload this to Google Colab to run your scraper!")

def main():
    """Main setup function"""
    print("🚀 Upload NBA Data Scraping to GitHub")
    print("="*50)
    print("Repository: https://github.com/LionelHutz123/funproject")
    print("="*50)
    
    # Check if we're in the right directory
    if not os.path.exists('main.py'):
        print("❌ Please run this script from your project directory")
        return
    
    # Setup git and push to GitHub
    print("\n📁 Setting up Git and uploading to GitHub...")
    if setup_git_repo():
        # Create Colab notebook
        print("\n📓 Creating Colab notebook...")
        create_colab_notebook()
        
        print("\n🎉 Setup complete!")
        print("\nNext steps:")
        print("1. Upload nba_scraping_colab.ipynb to Google Colab")
        print("2. Run the cells to start scraping!")
        print("3. Your repository: https://github.com/LionelHutz123/funproject")
    else:
        print("\n❌ Setup failed. You can:")
        print("1. Upload files manually via GitHub web interface")
        print("2. Check your git installation and permissions")

if __name__ == "__main__":
    main() 