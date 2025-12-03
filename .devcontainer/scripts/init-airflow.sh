#!/bin/bash
set -e

echo "=== Initializing Airflow ==="

cd "$(dirname "$0")/../../"

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
  if docker-compose exec -T airflow-postgres pg_isready -U airflow > /dev/null 2>&1; then
    echo "PostgreSQL is ready!"
    break
  fi
  echo "PostgreSQL is not ready yet, waiting... ($i/30)"
  sleep 2
done

# Wait for webserver to start
echo "Waiting for webserver to start..."
sleep 10

# Initialize the database
echo "Initializing Airflow database..."
docker-compose exec -T airflow-webserver airflow db init

# Create default admin user
echo "Creating default admin user..."
docker-compose exec -T airflow-webserver airflow users create \
  --role Admin \
  --username airflow \
  --email airflow@example.com \
  --firstname Airflow \
  --lastname Admin \
  --password airflow 2>/dev/null || echo "Admin user already exists"

echo "=== Airflow initialization complete ==="
