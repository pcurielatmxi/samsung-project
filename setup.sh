#!/bin/bash
# Setup script for WSL2 Ubuntu environment
# Run from project root: ./setup.sh
set -e

echo "=============================================="
echo "  MXI Samsung - WSL2 Environment Setup"
echo "=============================================="
echo ""

# Get project root (where this script lives)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

# Check Python version
echo "1. Checking Python..."
if ! command -v python3 &> /dev/null; then
    echo "   Python3 not found. Installing..."
    sudo apt update && sudo apt install -y python3 python3-pip python3-venv
else
    PYTHON_VERSION=$(python3 --version)
    echo "   Found: $PYTHON_VERSION"
fi

# Create virtual environment
echo ""
echo "2. Setting up virtual environment..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    echo "   Created .venv/"
else
    echo "   .venv/ already exists"
fi

# Activate virtual environment
source .venv/bin/activate
echo "   Activated virtual environment"

# Upgrade pip
echo ""
echo "3. Upgrading pip..."
pip install --upgrade pip --quiet

# Install Python dependencies
echo ""
echo "4. Installing Python dependencies..."
pip install -r requirements.txt --quiet
echo "   Installed $(pip list | wc -l) packages"

# Install Playwright browsers
echo ""
echo "5. Installing Playwright browsers..."
playwright install chromium

# Install Playwright system dependencies (needs sudo)
echo ""
echo "6. Installing Playwright system dependencies..."
sudo playwright install-deps chromium

# Setup environment file
echo ""
echo "7. Setting up environment..."
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "   Created .env from .env.example"
        echo "   ⚠️  Edit .env with your credentials"
    fi
else
    echo "   .env already exists"
fi

# Make scripts executable
echo ""
echo "8. Making scripts executable..."
chmod +x scripts/*.py 2>/dev/null || true
chmod +x setup.sh

echo ""
echo "=============================================="
echo "  Setup Complete!"
echo "=============================================="
echo ""
echo "To activate the environment:"
echo "  source .venv/bin/activate"
echo ""
echo "To test Playwright scraper:"
echo "  python scripts/scrape_projectsight_daily_reports.py --limit 10"
echo ""
echo "To process XER files:"
echo "  python scripts/batch_process_xer.py"
echo ""
