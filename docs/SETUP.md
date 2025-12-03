# Setup Instructions

## Prerequisites

- Docker Desktop (or Docker + Docker Compose)
- Python 3.11+ (for local development)
- Git

## Installation Steps

### 1. Environment Configuration

```bash
# Copy example configuration
cp .env.example .env

# Edit with your credentials
# nano .env  # or your preferred editor
```

Required environment variables:

```env
# ProjectSight credentials
PROJECTSIGHT_BASE_URL=https://projectsight.trimble.com
PROJECTSIGHT_USERNAME=your_username
PROJECTSIGHT_PASSWORD=your_password

# Fieldwire API
FIELDWIRE_API_KEY=your_api_key

# Database (default: PostgreSQL from docker-compose)
DB_HOST=postgres
DB_NAME=etl_db
DB_USER=airflow
DB_PASSWORD=airflow
```

### 2. Start Airflow Services

```bash
# Start all services (Airflow webserver, scheduler, PostgreSQL)
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f airflow-scheduler
```

### 3. Access Airflow UI

1. Open http://localhost:8080
2. Login with `airflow` / `airflow`
3. DAGs should appear after a few moments

### 4. Configure Connections (Optional)

In Airflow UI, add connections for:

**ProjectSight:**
- Conn ID: `projectsight_conn`
- Conn Type: HTTP
- Host: `https://projectsight.trimble.com`

**Fieldwire:**
- Conn ID: `fieldwire_conn`
- Conn Type: HTTP
- Host: `https://api.fieldwire.com`

**PostgreSQL:**
- Conn ID: `postgres_etl_db`
- Conn Type: Postgres
- Host: `postgres`
- Database: `etl_db`
- Login: `airflow`
- Password: `airflow`

### 5. Run a Test DAG

1. In Airflow UI, find `test_setup_dag`
2. Click "Trigger DAG" to verify setup
3. Check logs for successful execution

## Development Setup

### Local Installation (without Docker)

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Configure .env file

# Run tests
pytest

# Run specific component
python -c "from src.config.settings import settings; print(settings.PROJECTSIGHT_BASE_URL)"
```

### IDE Setup

**VS Code:**
1. Install Python extension
2. Select interpreter: `.venv/bin/python`
3. Tests will auto-discover

**PyCharm:**
1. File > Settings > Project > Python Interpreter
2. Click gear icon, Add Interpreter
3. Choose existing environment: `.venv`

## Troubleshooting

### Airflow won't start
```bash
# Check logs
docker-compose logs airflow-webserver

# Reset Airflow (careful - removes all data)
docker-compose down
docker volume rm mxi-samsung_postgres-db-volume
docker-compose up -d
```

### Database connection errors
```bash
# Test database connection
docker-compose exec postgres psql -U airflow -d etl_db -c "SELECT 1"

# Rebuild volumes
docker-compose down -v
docker-compose up -d
```

### Source system connection errors

**ProjectSight (web scraping):**
- Verify username/password are correct
- Check firewall/VPN access to ProjectSight
- Verify PROJECTSIGHT_BASE_URL is correct
- Check logs: `docker-compose logs airflow-scheduler`

**Fieldwire (API):**
- Verify API key is correct and has required permissions
- Check Fieldwire API status page
- Verify FIELDWIRE_BASE_URL is correct

### Python import errors
```bash
# Ensure project root is in PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:/path/to/mxi-samsung"

# Reinstall dependencies
pip install -e .
```

## Next Steps

1. Review [ETL_DESIGN.md](./ETL_DESIGN.md) for architecture details
2. Review [SOURCES.md](./SOURCES.md) for data source information
3. Implement extractors for your specific use cases
4. Create DAGs to orchestrate the ETL pipeline
5. Set up monitoring and alerting

## Useful Commands

```bash
# Airflow CLI (inside container)
docker-compose exec airflow-scheduler airflow dags list
docker-compose exec airflow-scheduler airflow tasks list test_setup_dag

# View specific DAG
docker-compose exec airflow-scheduler airflow dags show test_setup_dag

# Run a task manually
docker-compose exec airflow-scheduler airflow tasks run test_setup_dag print_hello 2025-01-01

# Access database
docker-compose exec postgres psql -U airflow -d etl_db

# View application logs
docker-compose logs -f airflow-scheduler --tail 100
```

## Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (data loss!)
docker-compose down -v

# Stop specific service
docker-compose stop airflow-scheduler
```
