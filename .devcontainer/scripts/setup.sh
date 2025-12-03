#!/bin/bash
set -e

echo "=== Setting up dev container ==="

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Make scripts executable
echo "Making scripts executable..."
chmod +x /workspaces/mxi-samsung/.devcontainer/scripts/*.sh

# Start Docker services
echo "Starting Docker services..."
/workspaces/mxi-samsung/.devcontainer/scripts/start-services.sh

echo "=== Dev container setup complete ==="
echo ""
echo "Airflow is starting up..."
echo "Webserver will be available at: http://localhost:8080"
echo "Default credentials: airflow / airflow"
echo ""
echo "Note: The initialization container will run automatically on first startup."
