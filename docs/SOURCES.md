# Data Sources Documentation

## ProjectSight (Trimble)

### Overview
- **Type:** Web Application (No public REST API)
- **Extraction Method:** Web Scraping (Selenium)
- **Authentication:** Username/Password login

### Connection Details

**Configuration:**
```
PROJECTSIGHT_BASE_URL=https://projectsight.trimble.com
PROJECTSIGHT_USERNAME=your_username
PROJECTSIGHT_PASSWORD=your_password
PROJECTSIGHT_TIMEOUT=30
PROJECTSIGHT_HEADLESS=true
```

### Available Data

#### Projects
- Project ID
- Project Name
- Status (Active, Inactive, On Hold, etc.)
- Start Date
- End Date
- Manager
- Client
- Budget
- Budget Used
- Location
- Description

**Extraction Endpoint:** Login → Projects List Page → Table Data

#### Resources
- Resource ID
- Resource Name
- Resource Type (Labor, Equipment, Material)
- Availability
- Cost/Rate
- Location

#### Schedule
- Task ID
- Task Name
- Assigned To
- Start Date
- End Date
- Status (Not Started, In Progress, Complete)
- Predecessor Tasks
- Duration

### Known Limitations

1. **No API:** Requires web scraping via Selenium
2. **Session Management:** May require re-authentication for long-running extractions
3. **Dynamic Content:** Heavy JavaScript rendering requires headless browser
4. **Rate Limits:** Excessive scraping may trigger blocking
5. **Data Consistency:** UI structure may change without notice

### Scraping Strategy

1. **Login Phase:**
   - Navigate to login page
   - Enter credentials
   - Wait for redirect to dashboard

2. **Data Collection:**
   - Navigate to desired view (Projects, Resources, etc.)
   - Handle pagination
   - Extract table rows/data
   - Parse HTML structure

3. **Error Handling:**
   - Session timeout detection
   - Missing element handling
   - Retry failed extractions
   - Log HTML structure changes

### Example Implementation Notes

```python
# Typical scraping steps
1. connector.authenticate()  # Login
2. connector.navigate_to('/projects')  # Go to projects page
3. projects = scrape_project_table()  # Extract from table
4. handle_pagination()  # If multiple pages
5. connector.close()  # Cleanup
```

## Fieldwire

### Overview
- **Type:** REST API
- **Extraction Method:** HTTP API Calls
- **Authentication:** API Key (Bearer Token)
- **Documentation:** https://apidocs.fieldwire.com

### Connection Details

**Configuration:**
```
FIELDWIRE_BASE_URL=https://api.fieldwire.com
FIELDWIRE_API_KEY=your_api_key_here
FIELDWIRE_TIMEOUT=30
FIELDWIRE_RETRY_ATTEMPTS=3
FIELDWIRE_RETRY_DELAY=5
FIELDWIRE_BATCH_SIZE=100
```

### Available Endpoints

#### Projects
**GET /v2/projects**

Response:
```json
{
  "id": "project_123",
  "name": "Downtown Office Building",
  "status": "active",
  "created_at": "2025-01-01T00:00:00Z",
  "updated_at": "2025-01-15T10:30:00Z",
  "location": "Downtown",
  "description": "Office renovation project"
}
```

#### Tasks
**GET /v2/projects/{project_id}/tasks**

Response:
```json
{
  "id": "task_456",
  "project_id": "project_123",
  "name": "Foundation Work",
  "status": "in_progress",
  "assignee_id": "user_789",
  "created_at": "2025-01-01T00:00:00Z",
  "updated_at": "2025-01-15T10:30:00Z",
  "due_date": "2025-02-01"
}
```

#### Workers
**GET /v2/projects/{project_id}/workers**

Response:
```json
{
  "id": "worker_999",
  "project_id": "project_123",
  "name": "John Doe",
  "email": "john@company.com",
  "role": "Supervisor",
  "status": "active",
  "created_at": "2025-01-01T00:00:00Z"
}
```

#### Checklists
**GET /v2/projects/{project_id}/checklists**

Response:
```json
{
  "id": "checklist_111",
  "project_id": "project_123",
  "name": "Safety Inspection",
  "completed": 8,
  "total": 10,
  "status": "in_progress",
  "created_at": "2025-01-01T00:00:00Z"
}
```

### API Features

#### Pagination
```
GET /v2/projects?page=1&per_page=100
```

Responses include:
- `page`: Current page number
- `per_page`: Results per page
- `total_count`: Total records
- `items`: Array of records

#### Filtering
```
GET /v2/projects?status=active
GET /v2/tasks?project_id=project_123
```

#### Rate Limits
- **Limit:** 1000 requests per hour
- **Headers:** `X-RateLimit-Limit`, `X-RateLimit-Remaining`
- **Strategy:** Implement backoff when limit approached

### Authentication

**Header:**
```
Authorization: Bearer {API_KEY}
```

### Error Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 400 | Bad Request |
| 401 | Unauthorized (invalid API key) |
| 403 | Forbidden (insufficient permissions) |
| 404 | Not Found |
| 429 | Too Many Requests (rate limit) |
| 500 | Server Error |

### Data Consistency Notes

1. **Timestamps:** Always in ISO 8601 UTC format
2. **IDs:** Unique identifiers, safe to use as primary keys
3. **Status Values:** Consistent across resource types
4. **Pagination:** Cursor-based recommended for large datasets
5. **Updates:** API maintains `updated_at` automatically

### Example Usage

```python
connector = APIConnector(
    name='Fieldwire',
    base_url='https://api.fieldwire.com',
    api_key='your_api_key'
)

# Get all projects
projects = connector.get('/v2/projects')

# Get tasks for a project
tasks = connector.get(f'/v2/projects/{project_id}/tasks')

# Get with pagination
page_data = connector.get('/v2/projects', params={'page': 1, 'per_page': 100})
```

## Data Integration Considerations

### Field Mapping

| ProjectSight | Fieldwire | Standardized |
|--------------|-----------|--------------|
| project_id | id | source_id |
| project_name | name | name |
| status | status | status |
| start_date | created_at | created_date |
| end_date | - | end_date |
| manager | assignee_id | assigned_to |

### Data Frequency

- **ProjectSight:** Daily extraction (slower due to scraping)
- **Fieldwire:** Hourly extraction (API-based)

### Storage Strategy

1. **Raw Data:** Store as-is in raw tables
2. **Normalized:** Transform to common schema
3. **Data Warehouse:** Denormalize for analytics

## Monitoring & Alerts

### Key Metrics

- Extraction duration
- Record counts per source
- Transformation success rate
- Load failures
- API error rates

### Alert Conditions

- Extraction takes > 2x normal time
- Record count deviates > 10% from baseline
- API returns > 5% errors
- Failed transformation validations
- Load operation failures
