# Quality Document Processor Configurations

This directory contains configurations for bulk processing quality inspection documents using the document processor tool.

## Overview

Two quality inspection systems are configured for automated data extraction:

1. **RABA (Raba Kistner)** - Laboratory testing and field quality control
2. **PSI (Professional Services Industries)** - Third-party field inspections

## Directory Structure

```
document_processor_configs/
├── README.md                 # This file
├── raba/
│   ├── schema.json          # JSON schema for RABA test reports
│   └── prompt.txt           # Extraction prompt for RABA reports
└── psi/
    ├── schema.json          # JSON schema for PSI daily field reports
    └── prompt.txt           # Extraction prompt for PSI reports
```

## Usage

### RABA Test Reports

**Source Directory:** `{WINDOWS_DATA_DIR}/raw/raba/daily/`
**Document Type:** Daily batch PDFs containing laboratory test reports
**Typical Tests:** Concrete compressive strength, soil compaction, fireproofing thickness, welding inspections, etc.

**Command:**
```bash
python scripts/document_processor/process_documents.py \
  -i "/mnt/c/Users/pdcur/OneDrive - MXI/Desktop/Samsung Dashboard/Data/raw/raba/daily" \
  -o "/mnt/c/Users/pdcur/OneDrive - MXI/Desktop/Samsung Dashboard/Data/processed/raba/structured" \
  -p "$(cat scripts/quality/document_processor_configs/raba/prompt.txt)" \
  --schema scripts/quality/document_processor_configs/raba/schema.json \
  --model sonnet \
  --concurrency 3 \
  --skip-existing
```

**Output:** One JSON file per PDF with structured test data including:
- Report metadata (date, project, test type, craft)
- Test sets/locations
- Individual test results with pass/fail status
- Summary statistics

### PSI Daily Field Reports

**Source Directory:** `{WINDOWS_DATA_DIR}/raw/psi/reports/`
**Document Type:** Individual daily field inspection reports (DFR)
**Typical Inspections:** Structural steel, framing, paint touch-up, bottom plates, etc.

**Command:**
```bash
python scripts/document_processor/process_documents.py \
  -i "/mnt/c/Users/pdcur/OneDrive - MXI/Desktop/Samsung Dashboard/Data/raw/psi/reports" \
  -o "/mnt/c/Users/pdcur/OneDrive - MXI/Desktop/Samsung Dashboard/Data/processed/psi/structured" \
  -p "$(cat scripts/quality/document_processor_configs/psi/prompt.txt)" \
  --schema scripts/quality/document_processor_configs/psi/schema.json \
  --model sonnet \
  --concurrency 5 \
  --skip-existing
```

**Output:** One JSON file per PDF with structured inspection data including:
- Report metadata (DFR number, issue date, reviewer)
- Inspection details (date, time, location, contractor)
- Work Inspection Request (WIR) details
- Inspection results and re-inspection status
- Deficiencies found and corrected
- Photo documentation flags

## Schema Design Philosophy

Both schemas are designed to be **flexible and extensible** to accommodate multiple test types and crafts:

### RABA Schema Flexibility

- **`test_type`**: Identifies the specific test (e.g., "Compressive Strength Test", "Welding Inspection")
- **`craft`**: Identifies the discipline (e.g., "Concrete", "Steel", "Fireproofing")
- **`material_specifications`**: Object with `additionalProperties: true` - captures test-specific material properties
- **`test_parameters`**: Object with `additionalProperties: true` - captures test-specific measurements

This allows the same schema to handle:
- Concrete compressive strength tests with slump, air content, strength data
- Fireproofing thickness with product info, application dates, thickness readings
- Welding inspections with weld IDs, procedures, defect types
- Soil compaction with moisture content, density, lift numbers

### PSI Schema Flexibility

- **`inspection_type.primary_type`**: Free-form text describing what was inspected
- **`discipline`**: Enum for common disciplines (Arch, Elec, Mech, Civil, Structural)
- **`deficiencies`**: Array structure to capture multiple issues found
- **`work_inspection_request`**: Full WIR details when formal inspection requests are documented
- **Photos tracking**: Captures presence and count of photo documentation

## Expected Document Volumes

| Source | Document Count | Estimated Processing Time (3-5 concurrency) |
|--------|---------------|---------------------------------------------|
| RABA daily batches | 995+ PDFs | ~6-8 hours (varies by batch size) |
| PSI reports | 6,309 PDFs | ~15-20 hours |

## Root Cause Analysis Capabilities

The enhanced schemas now capture structured data specifically designed for root cause analysis:

### RABA Test Reports - Root Cause Fields

| Field | Purpose | Example Values |
|-------|---------|----------------|
| `contractor` | Who performed the work being tested | "Yates", "SECAI", "Austin Bridge", subcontractor names |
| `material_supplier` | Material supplier for traceability | "Lauren Concrete", "Nucor Steel" |
| `batch_number` | Batch/lot/heat number for material tracking | "H12345", "Batch 2024-03-15-001" |
| `failure_reason` | Specific reason extracted from remarks | "Low strength - insufficient curing time" |
| `root_cause_indicators.category` | Primary failure category | Material, Workmanship, Design, Environmental, Mixed, Unknown |
| `root_cause_indicators.contributing_factors` | List of contributing factors | ["Cold weather during placement", "Delayed testing"] |
| `weather_conditions` | Weather during placement/installation | "Rainy", "Below 40°F", "High winds" |

### PSI Inspection Reports - Root Cause Fields

| Field | Purpose | Example Values |
|-------|---------|----------------|
| `inspection_details.contractor` | Contractor being inspected | "Yates", "Berg", "SECAI" |
| `deficiencies[].deficiency_type` | Category of deficiency | Missing Item, Improper Installation, Damaged Material, Out of Tolerance, Code Violation, Cleanliness, Incomplete Work |
| `deficiencies[].severity` | Criticality level | Critical, Major, Minor |
| `deficiencies[].responsible_party` | Who caused the deficiency | "Framing contractor", "Previous trade", specific subcontractor |
| `deficiencies[].root_cause_category` | Root cause classification | Material, Workmanship, Design/Coordination, Predecessor Work, Environmental, Unknown |
| `deficiencies[].corrected_during_inspection` | Immediate correction flag | true/false |

## Analysis Use Cases

After bulk processing, the structured JSON data enables:

### 1. Root Cause Analysis
- **Material Issues**: Group failures by supplier and batch number to identify defective material patterns
- **Workmanship Issues**: Correlate failures with specific contractors to identify training needs
- **Environmental Factors**: Identify weather-related failures (cold weather concrete, coating application)
- **Design Issues**: Find recurring problems that indicate specification or coordination issues

### 2. Contractor Performance
- Compare pass/fail rates across contractors (Yates, Berg, SECAI, subcontractors)
- Identify which contractors have highest deficiency rates
- Track whether deficiencies are corrected immediately vs requiring re-inspection
- Measure rework burden by contractor

### 3. Deficiency Pattern Analysis
- Most common deficiency types (Missing Items, Improper Installation, etc.)
- Which deficiency types correlate with re-work and delays
- Severity distribution (Critical vs Major vs Minor issues)
- Deficiency clustering by location or system

### 4. Volume and Timeline Trends
- Inspection counts by date, contractor, location, test type
- When quality issues peaked during project phases
- Re-inspection tracking as measure of rework

### 5. Supplier and Material Traceability
- Identify problematic material batches or suppliers
- Correlate material failures with specific test types
- Track if certain suppliers consistently deliver subpar materials

### 6. Cross-Dataset Correlation
- Link RABA test failures with PSI field observations
- Correlate quality issues with schedule delays (using Primavera data)
- Connect quality problems to labor consumption spikes (using ProjectSight hours data)

## Schema Validation

Both schemas follow JSON Schema Draft 07 specification. To validate output:

```bash
# Install jsonschema validator
pip install jsonschema

# Validate a processed file
python -c "import json, jsonschema; \
  schema = json.load(open('scripts/quality/document_processor_configs/raba/schema.json')); \
  data = json.load(open('output_file.json')); \
  jsonschema.validate(data, schema); \
  print('Valid!')"
```

## Notes

- **Idempotency**: Use `--skip-existing` flag to resume interrupted batch processing
- **Token Limits**: Default max is 100K tokens per document (configurable in document_processor script)
- **Error Handling**: Documents that fail processing are logged; check logs for rate limits or parsing errors
- **Model Selection**:
  - `sonnet` (default) - Best balance of speed and accuracy
  - `opus` - Highest accuracy for complex multi-page documents
  - `haiku` - Fastest for simple single-page reports

## Unified Format Option

Both RABA and PSI data can be transformed into a **single unified quality events table** for easier querying.

### Recommended Workflow

**Step 1:** Process documents with specialized schemas (best extraction accuracy):
```bash
# Process RABA with RABA schema
python scripts/document_processor/process_documents.py \
  -i "/path/to/raba/pdfs" \
  -o "/path/to/raba/json" \
  -p "$(cat scripts/quality/document_processor_configs/raba/prompt.txt)" \
  --schema scripts/quality/document_processor_configs/raba/schema.json

# Process PSI with PSI schema
python scripts/document_processor/process_documents.py \
  -i "/path/to/psi/pdfs" \
  -o "/path/to/psi/json" \
  -p "$(cat scripts/quality/document_processor_configs/psi/prompt.txt)" \
  --schema scripts/quality/document_processor_configs/psi/schema.json
```

**Step 2:** Transform to unified format (enables single-table analysis):
```bash
python scripts/quality/document_processor_configs/transform_to_unified.py \
  --raba-dir "/path/to/raba/json" \
  --psi-dir "/path/to/psi/json" \
  --output "/path/to/unified_quality_events.jsonl" \
  --output-format jsonl
```

**Step 3:** Load into analysis tool:
```python
import pandas as pd
import json

# Load unified events
events = []
with open('unified_quality_events.jsonl', 'r') as f:
    for line in f:
        events.append(json.loads(line))

df = pd.json_normalize(events)

# Now you can analyze both RABA and PSI together
failure_by_contractor = df[df['overall_result.status'] == 'Failed'].groupby('parties.contractor').size()
```

### Unified Schema Benefits

**Pros:**
- Single table for all quality events
- Easier to query across both sources
- Consistent failure_reason field for both RABA and PSI
- Standardized root cause categories
- Simplified contractor performance analysis

**Cons:**
- Some RABA-specific fields (test_parameters) and PSI-specific fields (deficiency_classification) in nested objects
- Initial transformation step required
- Less type safety (numeric vs categorical results in same field)

### Files

- **[unified_quality_event_schema.json](unified_quality_event_schema.json)** - Unified schema definition
- **[transform_to_unified.py](transform_to_unified.py)** - Transformation script

## Future Enhancements

Potential improvements to schemas and prompts:
- Add photo extraction and analysis
- Include drawing reference parsing
- Cross-reference WIR numbers with QC Logs database
- Add geospatial parsing for more precise location identification
- Enhance automated root cause classification using historical patterns
