#!/bin/bash
set -e

echo "=== Setting up dev container ==="

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Install Playwright browsers and dependencies
echo "Installing Playwright browsers..."
playwright install chromium

echo "Installing Playwright system dependencies..."
playwright install-deps chromium

# Make scripts executable
echo "Making scripts executable..."
chmod +x /workspaces/mxi-samsung/.devcontainer/scripts/*.sh 2>/dev/null || true

echo "=== Dev container setup complete ==="
