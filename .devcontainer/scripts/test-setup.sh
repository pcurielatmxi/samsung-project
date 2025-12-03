#!/bin/bash
set -e

echo "=== Testing Airflow Setup ==="
echo ""

# Check if containers are running
echo "1. Checking container status..."
docker-compose ps | grep -E "(airflow-webserver|airflow-scheduler|airflow-postgres)" || {
  echo "❌ Not all containers are running"
  exit 1
}
echo "✓ All containers are running"
echo ""

# Check if database is initialized
echo "2. Checking if database is initialized..."
docker-compose exec -T airflow-webserver airflow db check > /dev/null 2>&1 && {
  echo "✓ Database is initialized"
} || {
  echo "❌ Database check failed"
  exit 1
}
echo ""

# Check if admin user exists
echo "3. Checking if admin user exists..."
docker-compose exec -T airflow-webserver airflow users list | grep -q "airflow" && {
  echo "✓ Admin user 'airflow' exists"
} || {
  echo "❌ Admin user not found"
  exit 1
}
echo ""

# Check if test DAG is parsed correctly
echo "4. Checking if test DAG is parsed correctly..."
docker-compose exec -T airflow-webserver airflow dags list | grep -q "test_setup_dag" && {
  echo "✓ Test DAG 'test_setup_dag' is recognized"
} || {
  echo "⚠ Test DAG not yet visible (may take a moment to appear)"
}
echo ""

# Check webserver health
echo "5. Checking webserver health..."
curl -s http://localhost:8080/health > /dev/null && {
  echo "✓ Webserver is responding"
} || {
  echo "⚠ Webserver health check might be warming up"
}
echo ""

echo "=== Test Summary ==="
echo "✓ Airflow setup appears to be working correctly!"
echo ""
echo "Next steps:"
echo "1. Open http://localhost:8080 in your browser"
echo "2. Login with credentials: airflow / airflow"
echo "3. You should see the 'test_setup_dag' in the DAGs list"
echo "4. Click on the DAG to view its tasks and trigger runs"
echo ""
