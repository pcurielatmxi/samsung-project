# ETL Architecture & Design

## Overview

This ETL pipeline follows a modular, scalable architecture designed to:
- Support multiple data sources (ProjectSight, Fieldwire, and future sources)
- Maintain clear separation between extraction, transformation, and loading
- Enable easy testing and validation at each stage
- Provide comprehensive logging and error handling

## Architecture Layers

### 1. Connectors (src/connectors/)

**Responsibility:** Handle low-level communication with external systems

#### BaseConnector
Abstract base class defining the connector interface:
- `authenticate()` - Establish connection/login
- `validate_connection()` - Test connection health
- `close()` - Clean up resources

#### APIConnector (api_connector.py)
For REST API integrations:
- Manages HTTP sessions with retry logic
- Handles authentication (API keys, bearer tokens)
- Provides get/post methods
- Implements exponential backoff for failed requests

#### WebScraperConnector (web_scraper.py)
For web-based applications without APIs:
- Uses Selenium WebDriver for browser automation
- Handles login/authentication
- Provides element finding and interaction methods
- Supports headless mode for server environments

### 2. Extractors (src/extractors/)

**Responsibility:** Extract data from source systems using connectors

#### BaseExtractor
Abstract base class:
- `extract(**kwargs)` - Main extraction logic
- `validate_extraction(data)` - Validate extracted data
- `log_extraction()` - Track extraction metadata

#### System-Specific Extractors
Example: ProjectSightExtractor
- Uses WebScraperConnector to navigate and scrape data
- Implements pagination handling
- Validates all required fields present
- Returns list of dictionaries with standardized structure

Example: FieldwireExtractor
- Uses APIConnector to fetch data
- Implements support for multiple resource types (projects, tasks, etc.)
- Handles API pagination
- Validates response data

### 3. Transformers (src/transformers/)

**Responsibility:** Transform raw data into standardized format

#### BaseTransformer
Abstract base class:
- `transform(data)` - Transform data
- `validate_transformation(data)` - Validate transformed structure

#### System-Specific Transformers
Example transformations:
- Normalize field names across systems
- Convert dates to ISO format
- Handle null/missing values
- Add metadata (extraction_date, source, etc.)
- Flatten nested structures if needed

**Why separate transformers?**
- Different sources have different field semantics
- Source-specific validation rules
- Enables incremental enhancement without affecting extractors

### 4. Loaders (src/loaders/)

**Responsibility:** Load processed data to destination systems

#### BaseLoader
Abstract base class:
- `load(data, **kwargs)` - Load data to destination
- `validate_load(record_count)` - Verify load success
- `get_load_stats()` - Return load statistics

#### DatabaseLoader (db_loader.py)
For PostgreSQL/SQL databases:
- Uses psycopg2 for connection management
- Implements UPSERT (INSERT ... ON CONFLICT)
- Batch insert for performance
- Transaction management with rollback on errors

#### FileLoader (file_loader.py)
For file-based outputs:
- CSV format with pandas
- Parquet format (columnar, compressed)
- JSON format for debugging
- Creates output directories as needed

## Data Flow Example

```
ProjectSight Website
        ↓
[WebScraperConnector]
        ↓
[ProjectSightExtractor]
        ↓
Raw Data: [{"project_id": "1", "project_name": "Test", ...}]
        ↓
[ProjectSightTransformer]
        ↓
Standardized Data: [{"source": "projectsight", "source_id": "1", "name": "Test", ...}]
        ↓
[DatabaseLoader] or [FileLoader]
        ↓
PostgreSQL Table / CSV File
```

## Airflow DAG Structure

### ETL Pipeline DAG

Typical DAG structure:

```python
start
  ↓
[extract_projectsight] → [transform_projectsight] → [load_projectsight]
  ↓                                                    ↓
  [extract_fieldwire] → [transform_fieldwire] → [load_fieldwire]
        ↓                      ↓
       end_success
```

### Task Types

**Extract Tasks:** PythonOperator
```python
def extract_task():
    extractor = ProjectSightExtractor()
    data = extractor.extract()
    if not extractor.validate_extraction(data):
        raise ValueError("Extraction validation failed")
    return data
```

**Transform Tasks:** PythonOperator with XCom
```python
def transform_task(**context):
    raw_data = context['task_instance'].xcom_pull(task_ids='extract')
    transformer = ProjectSightTransformer()
    data = transformer.transform(raw_data)
    if not transformer.validate_transformation(data):
        raise ValueError("Transformation validation failed")
    return data
```

**Load Tasks:** PythonOperator
```python
def load_task(**context):
    data = context['task_instance'].xcom_pull(task_ids='transform')
    loader = DatabaseLoader()
    if not loader.load(data, table_name='projects'):
        raise ValueError("Load failed")
```

## Error Handling

### Validation at Each Stage

1. **Extraction Validation:**
   - Verify all required fields present
   - Check data types
   - Validate against expected schema

2. **Transformation Validation:**
   - Ensure standardized field names
   - Validate transformed data structure
   - Check for data loss during transformation

3. **Load Validation:**
   - Verify record count matches
   - Check database constraints
   - Validate loaded data integrity

### Retry Logic

- **Connector retries:** Exponential backoff for API calls
- **Airflow retries:** Configured at DAG level for task failures
- **Custom retries:** Helper function `retry_on_exception()` in utils

### Logging

```python
from src.utils.logger import configure_logging

logger = configure_logging(__name__)
logger.info(f"Extracted {count} records")
logger.warning("Field 'status' was null")
logger.error("Failed to connect to API")
```

## Adding New Data Sources

### Step 1: Understand the Source
- API or web-based?
- Authentication method?
- Rate limits?
- Data schema?

### Step 2: Create Connector
```python
# src/connectors/my_system_connector.py
class MySystemConnector(APIConnector):  # or WebScraperConnector
    def authenticate(self):
        # Implementation
        pass
```

### Step 3: Create Extractor
```python
# src/extractors/system_specific/my_system_extractor.py
class MySystemExtractor(BaseExtractor):
    def __init__(self):
        super().__init__('my_system')
        self.connector = MySystemConnector(...)

    def extract(self, **kwargs):
        # Implementation
        pass
```

### Step 4: Create Transformer
```python
# src/transformers/system_specific/my_system_transformer.py
class MySystemTransformer(BaseTransformer):
    def transform(self, data):
        # Implementation
        pass
```

### Step 5: Create DAG
```python
# dags/etl_my_system_dag.py
dag = DAG('etl_my_system', ...)
# Define tasks using extractors, transformers, loaders
```

### Step 6: Add Tests
```python
# tests/unit/test_my_system.py
class TestMySystemExtractor:
    # Test cases
    pass
```

## Best Practices

1. **Separation of Concerns:** Don't mix extraction, transformation, and loading logic
2. **Idempotency:** Tasks should produce same result when run multiple times
3. **Logging:** Log at INFO level for normal operations, DEBUG for detailed info
4. **Error Messages:** Include context (what failed and why) in error messages
5. **Configuration:** Use environment variables, never hardcode credentials
6. **Testing:** Test each component independently and integration together
7. **Documentation:** Document expected data schema and transformations
8. **Monitoring:** Track extraction counts, transformation quality, load success rates

## Performance Considerations

- **Batch Size:** Configure batch_size for APIs and database inserts
- **Pagination:** Implement efficient pagination for large datasets
- **Connection Pooling:** Reuse database connections
- **Caching:** Cache authentication tokens when possible
- **Compression:** Use Parquet format for large file outputs

## Security

- **Credentials:** Store in environment variables or Airflow Connections
- **HTTPS:** Use SSL/TLS for API connections
- **SQL Injection:** Use parameterized queries (psycopg2 execute_values)
- **Secrets Management:** Use Airflow secrets backend for sensitive data
- **Logging:** Don't log sensitive data (passwords, tokens, PII)
