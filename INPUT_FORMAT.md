# Input Format Documentation

This document describes the expected input file formats for the BigQuery schema documentation tool.

## Overview

The tool processes manually exported BigQuery schema information. You need to export schema metadata from BigQuery and save it in one of the supported formats before running the tool.

## Supported Formats

### 1. CSV Format

The CSV format should contain table and column schema information. Each row represents a column in a table.

#### Expected CSV Structure

The CSV file should have the following columns (order may vary):

- `table_name` or `table_catalog.table_schema.table_name` - Full table identifier
- `column_name` - Name of the column
- `data_type` - BigQuery data type (e.g., STRING, INTEGER, TIMESTAMP, ARRAY, STRUCT)
- `mode` - Column mode: NULLABLE, REQUIRED, or REPEATED
- `description` - Column description (optional)
- `table_description` - Table description (optional)

#### Example CSV Export Query

You can use the following BigQuery SQL query to export schema information:

```sql
SELECT
  CONCAT(c.table_catalog, '.', c.table_schema, '.', c.table_name) AS table_id,
  c.table_name,
  CAST(NULL AS STRING) AS table_description,
  c.column_name,
  c.data_type,
  c.is_nullable,
  CASE 
    WHEN c.is_nullable = 'YES' THEN 'NULLABLE'
    ELSE 'REQUIRED'
  END AS mode,
  c.column_default,
  c.description AS column_description
FROM
  `project_id.dataset_name.INFORMATION_SCHEMA.COLUMNS` c
WHERE
  c.table_schema = 'dataset_name'
ORDER BY
  c.table_name, c.ordinal_position;
```

**Note:** BigQuery's `INFORMATION_SCHEMA.TABLES` doesn't include a `table_description` column. The query above sets `table_description` to NULL. If you need table descriptions, you can:
1. Add them manually after export
2. Use the BigQuery API to get table metadata
3. Query dataset-specific metadata if available

**Note:** The correct format is `project_id.dataset_name.INFORMATION_SCHEMA.COLUMNS`, not `project_id.dataset_name.table_name.INFORMATION_SCHEMA.COLUMNS`. The table name goes in the WHERE clause, not in the FROM clause.

**Example for a specific table:**

```sql
SELECT
  CONCAT(c.table_catalog, '.', c.table_schema, '.', c.table_name) AS table_id,
  c.table_name,
  CAST(NULL AS STRING) AS table_description,
  c.column_name,
  c.data_type,
  c.is_nullable,
  CASE 
    WHEN c.is_nullable = 'YES' THEN 'NULLABLE'
    ELSE 'REQUIRED'
  END AS mode,
  c.column_default,
  c.description AS column_description
FROM
  `reporting-224812.mpm_postgresql_dwh_prod.INFORMATION_SCHEMA.COLUMNS` c
WHERE
  c.table_name = 'visit'
ORDER BY
  c.table_name, c.ordinal_position;
```

**Note:** BigQuery's `INFORMATION_SCHEMA.TABLES` doesn't have a `table_description` column. If you need table descriptions, you'll need to add them manually after export, or use a different metadata source. The query above sets `table_description` to NULL as a placeholder.

**To export all tables in a dataset:**

```sql
SELECT
  CONCAT(c.table_catalog, '.', c.table_schema, '.', c.table_name) AS table_id,
  c.table_name,
  CAST(NULL AS STRING) AS table_description,
  c.column_name,
  c.data_type,
  c.is_nullable,
  CASE 
    WHEN c.is_nullable = 'YES' THEN 'NULLABLE'
    ELSE 'REQUIRED'
  END AS mode,
  c.column_default,
  c.description AS column_description
FROM
  `reporting-224812.mpm_postgresql_dwh_prod.INFORMATION_SCHEMA.COLUMNS` c
ORDER BY
  c.table_name, c.ordinal_position;
```

#### Export Steps

1. Run the query in BigQuery console
2. Click "Save results" → "Download as CSV"
3. Save the file with a descriptive name (e.g., `schema_export.csv`)

### 2. JSON Format

The JSON format should contain structured schema information, either as a single object or array of table definitions.

#### Expected JSON Structure

**Option A: Array of Tables**

```json
[
  {
    "table_name": "users",
    "table_description": "User information table",
    "columns": [
      {
        "name": "user_id",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Unique user identifier"
      },
      {
        "name": "email",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "User email address"
      }
    ]
  }
]
```

**Option B: BigQuery Schema Export Format**

If you export the schema directly from BigQuery (using the BigQuery API or `bq show --schema`), the format will be:

```json
[
  {
    "name": "column_name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Column description"
  }
]
```

In this case, you'll need separate files for each table, or a combined structure.

#### Example JSON Export

You can use the BigQuery CLI to export schema:

```bash
bq show --schema --format=prettyjson project_id:dataset_name.table_name > table_schema.json
```

Or use the BigQuery API to fetch schema information programmatically.

### 3. Directory of Files

You can also provide a directory containing multiple schema export files. The tool will:

- Process all CSV or JSON files in the directory
- Combine schema information from all files
- Generate unified documentation

## File Naming Conventions

- CSV files: `*.csv`
- JSON files: `*.json`
- The tool will auto-detect format based on file extension

## Multiple Dataset Support

If you have multiple datasets:

1. Export each dataset's schema separately
2. Place all export files in the same directory
3. The tool will process all files and combine them into a unified schema model

## Example File Structure

```
bigquery-exports/
├── dataset1_schema.csv
├── dataset2_schema.csv
└── dataset3_tables.json
```

## Tips for Manual Export

1. **Include Descriptions**: When exporting, make sure to include column and table descriptions as they are valuable for semantic mapping
2. **Consistent Format**: Use the same export format for all files to ensure consistent parsing
3. **Naming**: Use descriptive file names that indicate the dataset or table name
4. **Complete Data**: Export all tables you want to document, not just a subset

## Validation

Before running the tool, verify your export files:

- CSV files should have headers matching the expected column names
- JSON files should be valid JSON
- All required fields (table_name, column_name, data_type) should be present

