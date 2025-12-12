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

# Step 1: Install system dependencies
echo "1. Installing system dependencies..."
sudo apt update -qq
sudo apt install -y -qq \
    git \
    curl \
    wget \
    build-essential \
    python3 \
    python3-pip \
    python3-venv \
    libpq-dev \
    > /dev/null 2>&1
echo "   System dependencies installed"

# Step 1b: Install Claude Code CLI
echo ""
echo "1b. Installing Claude Code CLI..."
if ! command -v claude &> /dev/null; then
    curl -fsSL https://claude.ai/install.sh | bash
    echo "   Claude Code installed"
else
    echo "   Claude Code already installed"
fi

# Step 2: Create virtual environment
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

# Step 3: Upgrade pip
echo ""
echo "3. Upgrading pip..."
pip install --upgrade pip --quiet

# Step 4: Install Python dependencies
echo ""
echo "4. Installing Python dependencies..."
pip install -r requirements.txt --quiet
echo "   Installed $(pip list | wc -l) packages"

# Step 5: Install Playwright browsers
echo ""
echo "5. Installing Playwright browsers..."
playwright install chromium

# Step 6: Install Playwright system dependencies
echo ""
echo "6. Installing Playwright system dependencies..."
sudo "$PROJECT_ROOT/.venv/bin/playwright" install-deps chromium

# Step 7: Setup environment file
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

# Step 8: Make scripts executable
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
