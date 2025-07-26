#!/usr/bin/env python3
"""
GitHub repository setup helper
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

def init_git_repo():
    """Initialize git repository"""
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
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Git error: {e}")
        return False

def create_github_repo_instructions():
    """Print instructions for creating GitHub repository"""
    print("\n" + "="*60)
    print("GITHUB REPOSITORY SETUP")
    print("="*60)
    print("\nFollow these steps to create your GitHub repository:")
    print("\n1. Go to https://github.com")
    print("2. Click 'New repository'")
    print("3. Name it: nba-data-scraping")
    print("4. Make it Public")
    print("5. Don't initialize with README (we already have one)")
    print("6. Click 'Create repository'")
    print("\nAfter creating the repository, run:")
    print("git remote add origin https://github.com/YOUR_USERNAME/nba-data-scraping.git")
    print("git branch -M main")
    print("git push -u origin main")

def setup_github_remote():
    """Setup GitHub remote repository"""
    username = input("Enter your GitHub username: ")
    repo_name = input("Enter repository name (default: nba-data-scraping): ") or "nba-data-scraping"
    
    remote_url = f"https://github.com/{username}/{repo_name}.git"
    
    try:
        # Add remote
        subprocess.run(['git', 'remote', 'add', 'origin', remote_url], check=True)
        print(f"✅ Remote added: {remote_url}")
        
        # Set main branch
        subprocess.run(['git', 'branch', '-M', 'main'], check=True)
        print("✅ Main branch set")
        
        # Push to GitHub
        subprocess.run(['git', 'push', '-u', 'origin', 'main'], check=True)
        print("✅ Code pushed to GitHub!")
        
        print(f"\n🎉 Your repository is now available at:")
        print(f"https://github.com/{username}/{repo_name}")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")
        print("Make sure you've created the repository on GitHub first.")
        return False

def create_colab_notebook():
    """Create a Colab notebook that clones from GitHub"""
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
    "This notebook runs the NBA data scraping system directly from GitHub."
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
    "# Clone the repository from GitHub\n",
    "# Replace YOUR_USERNAME with your actual GitHub username\n",
    "!git clone https://github.com/YOUR_USERNAME/nba-data-scraping.git\n",
    "%cd nba-data-scraping\n",
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
    print("Upload this to Google Colab and update YOUR_USERNAME")

def main():
    """Main setup function"""
    print("🚀 GitHub Repository Setup for NBA Data Scraping")
    print("="*60)
    
    # Check if we're in the right directory
    if not os.path.exists('main.py'):
        print("❌ Please run this script from your project directory")
        return
    
    # Initialize git repository
    print("\n📁 Setting up Git repository...")
    if not init_git_repo():
        return
    
    # Show GitHub creation instructions
    create_github_repo_instructions()
    
    # Ask if user wants to setup remote
    response = input("\nHave you created the GitHub repository? (y/N): ")
    if response.lower() == 'y':
        if setup_github_remote():
            # Create Colab notebook
            print("\n📓 Creating Colab notebook...")
            create_colab_notebook()
            
            print("\n🎉 Setup complete!")
            print("\nNext steps:")
            print("1. Upload nba_scraping_colab.ipynb to Google Colab")
            print("2. Update YOUR_USERNAME in the notebook")
            print("3. Run the cells to start scraping!")
    else:
        print("\nNo problem! You can:")
        print("1. Create the repository manually on GitHub")
        print("2. Run: git remote add origin YOUR_REPO_URL")
        print("3. Run: git push -u origin main")

if __name__ == "__main__":
    main() 