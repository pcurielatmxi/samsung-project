# Implementation Guide

This guide walks you through implementing the actual data extraction, transformation, and loading logic for ProjectSight and Fieldwire.

## Phase 1: Setup & Configuration

### Step 1: Configure Environment Variables

```bash
cp .env.example .env
```

Edit `.env` with your actual credentials:

```env
# ProjectSight (Trimble)
PROJECTSIGHT_BASE_URL=https://your-projectsight-instance.trimble.com
PROJECTSIGHT_USERNAME=your_username
PROJECTSIGHT_PASSWORD=your_password

# Fieldwire API
FIELDWIRE_API_KEY=your_api_key_here

# Database (already configured for docker-compose)
DB_HOST=postgres
DB_NAME=etl_db
DB_USER=airflow
DB_PASSWORD=airflow
```

### Step 2: Start Airflow

```bash
docker-compose up -d
```

Verify all services are running:
```bash
docker-compose ps
```

## Phase 2: Implement ProjectSight Extractor

### Understanding ProjectSight Structure

ProjectSight is a web-based application, so extraction requires web scraping. Key areas to scrape:
1. **Login page** - Authenticate with username/password
2. **Projects list** - Table with all projects
3. **Project details** - Individual project information
4. **Resources** - Equipment and personnel assignments

### Implementation Steps

#### Step 1: Test Connection

```python
# scripts/test_projectsight_connection.py
from src.connectors.web_scraper import WebScraperConnector
from src.config.settings import settings

connector = WebScraperConnector(
    name='ProjectSight',
    base_url=settings.PROJECTSIGHT_BASE_URL,
    username=settings.PROJECTSIGHT_USERNAME,
    password=settings.PROJECTSIGHT_PASSWORD,
    headless=False  # Use headless=False to see what's happening
)

# Test connection
if connector.authenticate():
    if connector.validate_connection():
        print("✓ ProjectSight connection successful")
    else:
        print("✗ Connection validation failed")
else:
    print("✗ Authentication failed")

connector.close()
```

Run: `python scripts/test_projectsight_connection.py`

#### Step 2: Implement Login Logic

In [src/extractors/system_specific/projectsight_extractor.py](src/extractors/system_specific/projectsight_extractor.py):

```python
def _login(self) -> bool:
    """Log into ProjectSight."""
    try:
        # Navigate to login page
        self.connector.navigate_to(f'{self.connector.base_url}/login')

        # Find and fill username field
        username_field = self.connector.find_element(
            By.ID, 'username'  # Adjust selector based on actual HTML
        )
        self.connector.send_keys(username_field, self.connector.username)

        # Find and fill password field
        password_field = self.connector.find_element(
            By.ID, 'password'
        )
        self.connector.send_keys(password_field, self.connector.password)

        # Find and click login button
        login_button = self.connector.find_element(
            By.XPATH, '//button[contains(text(), "Login")]'
        )
        self.connector.click_element(login_button)

        # Wait for redirect
        time.sleep(2)

        self.logger.info("Successfully logged into ProjectSight")
        return True
    except Exception as e:
        self.logger.error(f"Login failed: {str(e)}")
        return False
```

#### Step 3: Implement Data Scraping

```python
def _scrape_project_table(self) -> List[Dict[str, Any]]:
    """Scrape project table from the projects list page."""
    projects = []

    try:
        # Navigate to projects page
        self.connector.navigate_to(f'{self.connector.base_url}/projects')
        time.sleep(2)  # Wait for page load

        # Find all project rows in table
        rows = self.connector.find_elements(
            By.XPATH, '//table[@id="projectsTable"]//tbody/tr'
        )

        self.logger.info(f"Found {len(rows)} project rows")

        for idx, row in enumerate(rows):
            try:
                # Extract cells
                cells = row.find_elements(By.TAG_NAME, 'td')

                project = {
                    'project_id': self.connector.extract_text(cells[0]),
                    'project_name': self.connector.extract_text(cells[1]),
                    'status': self.connector.extract_text(cells[2]),
                    'manager': self.connector.extract_text(cells[3]),
                    'start_date': self.connector.extract_text(cells[4]),
                    'end_date': self.connector.extract_text(cells[5]),
                }

                projects.append(project)
                self.logger.debug(f"Extracted project {idx + 1}: {project['project_id']}")

            except Exception as e:
                self.logger.warning(f"Failed to extract row {idx}: {str(e)}")
                continue

        return projects

    except Exception as e:
        self.logger.error(f"Table scraping failed: {str(e)}")
        return []
```

#### Step 4: Test Extraction

```bash
python -c "
from src.extractors.system_specific.projectsight_extractor import ProjectSightExtractor

extractor = ProjectSightExtractor()
try:
    projects = extractor.extract()
    print(f'Extracted {len(projects)} projects')
    if projects:
        print('Sample:', projects[0])
except Exception as e:
    print(f'Error: {e}')
"
```

## Phase 3: Implement Fieldwire Extractor

### Understanding Fieldwire API

Fieldwire provides REST API endpoints. Key resources:
- `/v2/projects` - List projects
- `/v2/projects/{id}/tasks` - Project tasks
- `/v2/projects/{id}/workers` - Project workers
- `/v2/projects/{id}/checklists` - Project checklists

### Implementation Steps

#### Step 1: Test API Connection

```bash
curl -H "Authorization: Bearer YOUR_API_KEY" \
  https://api.fieldwire.com/v2/projects
```

Or in Python:

```python
# scripts/test_fieldwire_connection.py
from src.connectors.api_connector import APIConnector
from src.config.settings import settings

connector = APIConnector(
    name='Fieldwire',
    base_url=settings.FIELDWIRE_BASE_URL,
    api_key=settings.FIELDWIRE_API_KEY,
)

if connector.authenticate():
    if connector.validate_connection():
        print("✓ Fieldwire connection successful")
    else:
        print("✗ Connection validation failed")
else:
    print("✗ Authentication failed")
```

#### Step 2: Implement API Methods

In [src/extractors/system_specific/fieldwire_extractor.py](src/extractors/system_specific/fieldwire_extractor.py):

```python
def _extract_projects(self, **kwargs) -> List[Dict[str, Any]]:
    """Extract all projects from Fieldwire."""
    all_projects = []
    page = 1
    per_page = settings.FIELDWIRE_BATCH_SIZE

    try:
        while True:
            # Get projects page
            response = self.connector.get(
                '/v2/projects',
                params={'page': page, 'per_page': per_page}
            )

            # Add projects
            projects = response.get('data', [])
            if not projects:
                break

            all_projects.extend(projects)
            self.logger.info(f"Fetched {len(projects)} projects from page {page}")

            # Check if more pages
            total = response.get('meta', {}).get('total_count', 0)
            if len(all_projects) >= total:
                break

            page += 1
            time.sleep(1)  # Rate limiting

        return all_projects

    except Exception as e:
        self.logger.error(f"Failed to extract projects: {str(e)}")
        return []

def _extract_tasks(self, project_id: str = None, **kwargs) -> List[Dict[str, Any]]:
    """Extract tasks, optionally filtered by project."""
    all_tasks = []

    try:
        # If project_id provided, get only that project's tasks
        if project_id:
            response = self.connector.get(f'/v2/projects/{project_id}/tasks')
            return response.get('data', [])

        # Otherwise, get all projects and their tasks
        projects = self._extract_projects(**kwargs)

        for project in projects:
            try:
                response = self.connector.get(
                    f'/v2/projects/{project["id"]}/tasks'
                )
                tasks = response.get('data', [])

                # Add project_id to each task
                for task in tasks:
                    task['project_id'] = project['id']

                all_tasks.extend(tasks)
                self.logger.debug(f"Fetched {len(tasks)} tasks for project {project['id']}")

            except Exception as e:
                self.logger.warning(f"Failed to get tasks for project {project['id']}: {str(e)}")
                continue

        return all_tasks

    except Exception as e:
        self.logger.error(f"Failed to extract tasks: {str(e)}")
        return []
```

#### Step 3: Test Extraction

```bash
python -c "
from src.extractors.system_specific.fieldwire_extractor import FieldwireExtractor

extractor = FieldwireExtractor()
try:
    projects = extractor.extract(resource_type='projects')
    print(f'Extracted {len(projects)} projects')
    if projects:
        print('Sample:', projects[0])
except Exception as e:
    print(f'Error: {e}')
"
```

## Phase 4: Test Transformations

Both transformers are already implemented. Test them:

```python
from src.transformers.system_specific.fieldwire_transformer import FieldwireTransformer
from src.extractors.system_specific.fieldwire_extractor import FieldwireExtractor

# Extract raw data
extractor = FieldwireExtractor()
raw_data = extractor.extract(resource_type='projects')

# Transform
transformer = FieldwireTransformer()
transformed = transformer.transform(raw_data)

# Validate
if transformer.validate_transformation(transformed):
    print(f"✓ Transformed {len(transformed)} records successfully")
else:
    print("✗ Transformation validation failed")
```

## Phase 5: Create ETL DAGs

### ProjectSight ETL DAG

Create file: [dags/etl_projectsight_dag.py](dags/etl_projectsight_dag.py)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from dags.config import DEFAULT_ARGS, DEFAULT_DAG_CONFIG
from src.extractors.system_specific.projectsight_extractor import ProjectSightExtractor
from src.transformers.system_specific.projectsight_transformer import ProjectSightTransformer
from src.loaders.db_loader import DatabaseLoader

default_args = DEFAULT_ARGS.copy()
default_args['start_date'] = datetime(2025, 1, 1)

dag = DAG(
    'etl_projectsight',
    default_args=default_args,
    description='Extract ProjectSight data via web scraping',
    schedule_interval=timedelta(days=1),
    **DEFAULT_DAG_CONFIG,
)

def extract_projectsight():
    """Extract projects from ProjectSight."""
    extractor = ProjectSightExtractor()
    try:
        data = extractor.extract()
        if not extractor.validate_extraction(data):
            raise ValueError("Extraction validation failed")
        return data
    except Exception as e:
        raise

def transform_projectsight(**context):
    """Transform ProjectSight data."""
    raw_data = context['task_instance'].xcom_pull(
        task_ids='extract_projectsight'
    )

    transformer = ProjectSightTransformer()
    data = transformer.transform(raw_data)

    if not transformer.validate_transformation(data):
        raise ValueError("Transformation validation failed")

    return data

def load_projectsight(**context):
    """Load ProjectSight data to database."""
    data = context['task_instance'].xcom_pull(
        task_ids='transform_projectsight'
    )

    loader = DatabaseLoader()
    if not loader.load(data, table_name='projectsight_projects'):
        raise ValueError("Load operation failed")

# Define tasks
task_extract = PythonOperator(
    task_id='extract_projectsight',
    python_callable=extract_projectsight,
    dag=dag,
)

task_transform = PythonOperator(
    task_id='transform_projectsight',
    python_callable=transform_projectsight,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_projectsight',
    python_callable=load_projectsight,
    dag=dag,
)

# Dependencies
task_extract >> task_transform >> task_load
```

### Fieldwire ETL DAG

Create file: [dags/etl_fieldwire_dag.py](dags/etl_fieldwire_dag.py)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from dags.config import DEFAULT_ARGS, DEFAULT_DAG_CONFIG
from src.extractors.system_specific.fieldwire_extractor import FieldwireExtractor
from src.transformers.system_specific.fieldwire_transformer import FieldwireTransformer
from src.loaders.db_loader import DatabaseLoader

default_args = DEFAULT_ARGS.copy()
default_args['start_date'] = datetime(2025, 1, 1)

dag = DAG(
    'etl_fieldwire',
    default_args=default_args,
    description='Extract Fieldwire API data',
    schedule_interval=timedelta(hours=6),  # Every 6 hours
    **DEFAULT_DAG_CONFIG,
)

def extract_resource(resource_type='projects'):
    """Factory function to extract different resource types."""
    def extract():
        extractor = FieldwireExtractor()
        try:
            data = extractor.extract(resource_type=resource_type)
            if not extractor.validate_extraction(data):
                raise ValueError(f"Extraction validation failed for {resource_type}")
            return data
        except Exception as e:
            raise
    return extract

def transform_fieldwire(**context):
    """Transform Fieldwire data."""
    raw_data = context['task_instance'].xcom_pull(
        task_ids='extract_fieldwire_projects'
    )

    transformer = FieldwireTransformer()
    data = transformer.transform(raw_data)

    if not transformer.validate_transformation(data):
        raise ValueError("Transformation validation failed")

    return data

def load_fieldwire(**context):
    """Load Fieldwire data to database."""
    data = context['task_instance'].xcom_pull(
        task_ids='transform_fieldwire'
    )

    loader = DatabaseLoader()
    if not loader.load(data, table_name='fieldwire_projects'):
        raise ValueError("Load operation failed")

# Define tasks
task_extract = PythonOperator(
    task_id='extract_fieldwire_projects',
    python_callable=extract_resource('projects'),
    dag=dag,
)

task_transform = PythonOperator(
    task_id='transform_fieldwire',
    python_callable=transform_fieldwire,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_fieldwire',
    python_callable=load_fieldwire,
    dag=dag,
)

# Dependencies
task_extract >> task_transform >> task_load
```

## Phase 6: Set Up Database Schema

Connect to PostgreSQL and create tables:

```sql
-- ProjectSight projects table
CREATE TABLE IF NOT EXISTS projectsight_projects (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    status VARCHAR(50),
    start_date DATE,
    end_date DATE,
    extracted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fieldwire projects table
CREATE TABLE IF NOT EXISTS fieldwire_projects (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    status VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    extracted_at TIMESTAMP,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_projectsight_source_id ON projectsight_projects(source_id);
CREATE INDEX idx_fieldwire_source_id ON fieldwire_projects(source_id);
CREATE INDEX idx_projectsight_extracted ON projectsight_projects(extracted_at);
CREATE INDEX idx_fieldwire_extracted ON fieldwire_projects(extracted_at);
```

## Testing & Validation

### Run Unit Tests

```bash
# All tests
pytest

# Specific test file
pytest tests/unit/test_extractors.py -v

# With coverage
pytest --cov=src tests/
```

### Manual Testing

```bash
# Test ProjectSight extractor
python scripts/test_projectsight_extractor.py

# Test Fieldwire extractor
python scripts/test_fieldwire_extractor.py

# Test transformers
python scripts/test_transformers.py

# Test loaders
python scripts/test_loaders.py
```

## Monitoring & Troubleshooting

### Check Airflow Logs

```bash
# View DAG logs
docker-compose logs airflow-scheduler | grep etl_fieldwire

# View task logs
docker-compose exec airflow-scheduler \
  airflow logs etl_fieldwire extract_fieldwire_projects 2025-01-01
```

### Common Issues

1. **ProjectSight login fails**
   - Check username/password
   - Verify selectors match your instance (HTML may differ)
   - Use `headless=False` to debug visually

2. **Fieldwire API 401 (Unauthorized)**
   - Verify API key is correct
   - Check API key hasn't expired
   - Verify API key has required permissions

3. **Database connection refused**
   - Check PostgreSQL is running: `docker-compose ps`
   - Verify connection string in settings
   - Check database exists: `docker-compose exec postgres psql -U airflow -l`

4. **Data validation failures**
   - Check extractor output matches expected schema
   - Review transformer field mapping
   - Add logging to debug transformations

## Next: Production Readiness

After implementation:
1. Set up CI/CD pipeline (GitHub Actions)
2. Add comprehensive data quality checks
3. Configure monitoring and alerting
4. Set up backup/recovery procedures
5. Document data lineage and transformations
6. Create runbooks for common issues
