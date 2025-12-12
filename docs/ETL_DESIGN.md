# ETL Architecture & Design

## Overview

This project includes an ETL pipeline designed to:
- Support multiple data sources (Primavera, ProjectSight exports, Fieldwire API)
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

### 2. Extractors (src/extractors/)

**Responsibility:** Extract data from source systems using connectors

#### BaseExtractor
Abstract base class:
- `extract(**kwargs)` - Main extraction logic
- `validate_extraction(data)` - Validate extracted data
- `log_extraction()` - Track extraction metadata

#### System-Specific Extractors
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
Fieldwire API
      ↓
[APIConnector]
      ↓
[FieldwireExtractor]
      ↓
Raw Data: [{"id": "123", "name": "Test", ...}]
      ↓
[FieldwireTransformer]
      ↓
Standardized Data: [{"source": "fieldwire", "source_id": "123", "name": "Test", ...}]
      ↓
[DatabaseLoader] or [FileLoader]
      ↓
PostgreSQL Table / CSV File
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
- **Custom retries:** Helper function `retry_on_exception()` in utils

## Adding New Data Sources

### Step 1: Understand the Source
- API or file-based?
- Authentication method?
- Rate limits?
- Data schema?

### Step 2: Create Extractor
```python
# src/extractors/system_specific/my_system_extractor.py
class MySystemExtractor(BaseExtractor):
    def __init__(self):
        super().__init__('my_system')
        self.connector = APIConnector(...)

    def extract(self, **kwargs):
        # Implementation
        pass
```

### Step 3: Create Transformer
```python
# src/transformers/system_specific/my_system_transformer.py
class MySystemTransformer(BaseTransformer):
    def transform(self, data):
        # Implementation
        pass
```

### Step 4: Add Tests
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

## Performance Considerations

- **Batch Size:** Configure batch_size for APIs and database inserts
- **Pagination:** Implement efficient pagination for large datasets
- **Connection Pooling:** Reuse database connections
- **Caching:** Cache authentication tokens when possible
- **Compression:** Use Parquet format for large file outputs

## Security

- **Credentials:** Store in environment variables
- **HTTPS:** Use SSL/TLS for API connections
- **SQL Injection:** Use parameterized queries (psycopg2 execute_values)
- **Logging:** Don't log sensitive data (passwords, tokens, PII)
