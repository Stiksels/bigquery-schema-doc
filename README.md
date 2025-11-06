# BigQuery Schema Documentation Tool

A Python tool that processes manually exported BigQuery schema information and generates comprehensive documentation in multiple formats suitable for semantic mapping workshops.

## Overview

This tool helps you document your BigQuery database schema by:
- Processing manually exported schema files (CSV or JSON)
- Generating human-readable Markdown documentation
- Creating UML diagrams (PlantUML and Mermaid formats)
- Exporting structured formats (JSON, YAML, CSV) for programmatic analysis
- Identifying relationships between tables

## Installation

1. Clone or download this repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Manual BigQuery Export Process

Before using this tool, you need to export schema information from BigQuery manually:

1. **Export Schema Data**: Run queries in BigQuery to export table and column metadata
   - See `INPUT_FORMAT.md` for example queries and export formats
   - Export as CSV or JSON format
   
2. **Prepare Files**: Place exported files in a directory
   - Single file: point to the file directly
   - Multiple files: place in a directory and point to the directory

3. **Run the Tool**: Process the exports and generate documentation

## Usage

### Basic Usage

```bash
python main.py --input ./bigquery-exports --output-dir ./schema-docs
```

### Advanced Usage

```bash
python main.py \
  --input ./bigquery-exports \
  --input-format csv \
  --output-dir ./schema-docs \
  --formats text uml json yaml
```

### Command Line Options

- `--input`, `-i`: Path to input file or directory containing exported schema files (required)
- `--input-format`, `-f`: Input file format ('csv', 'json', or auto-detect if not specified)
- `--output-dir`, `-o`: Output directory for generated documentation (default: ./schema-docs)
- `--formats`: Output formats to generate (default: text, uml, json, yaml)
  - Options: `text`, `uml`, `json`, `yaml`, `csv`
  - Can specify multiple: `--formats text uml json`

### Examples

**Process a single CSV export:**
```bash
python main.py -i schema_export.csv -o ./docs
```

**Process multiple JSON files:**
```bash
python main.py -i ./exports -f json --formats text uml
```

**Generate all formats:**
```bash
python main.py -i ./bigquery-exports --formats text uml json yaml csv
```

## Output Files

The tool generates the following files in the output directory:

- **`schema_documentation.md`** - Comprehensive Markdown documentation
  - Dataset overview
  - Table-by-table documentation
  - Column listings with types and descriptions
  - Relationship mappings
  - Index of all tables and columns

- **`schema_diagram.puml`** - PlantUML diagram file
  - Can be rendered using PlantUML tools or online viewer
  - Shows tables as classes with relationships

- **`schema_diagram.mmd`** - Mermaid diagram file
  - Can be rendered in Markdown viewers, GitHub, or Mermaid Live Editor
  - Entity-relationship diagram format

- **`schema.json`** - Structured JSON schema definition
  - Machine-readable format
  - Suitable for programmatic analysis

- **`schema.yaml`** - Structured YAML schema definition
  - Human-friendly structured format
  - Easy to compare with other schemas

- **`schema_export.csv`** - CSV export of schema information
  - Tabular format for spreadsheet analysis
  - Includes tables, columns, and relationships

## Input Format Requirements

See `INPUT_FORMAT.md` for detailed information about:
- Expected CSV column structure
- JSON format specifications
- Example BigQuery export queries
- File naming conventions

## Use Cases

### Semantic Mapping Workshops

This tool is designed for preparing data models for semantic mapping workshops where you:
- Compare your data model with external parties
- Identify semantic links between resources
- Document relationships and mappings
- Generate visual representations for discussion

### Documentation

Generate comprehensive documentation of your BigQuery schema for:
- Team onboarding
- Data governance
- Schema evolution tracking
- Integration planning

## Features

- **Flexible Input**: Supports CSV and JSON export formats
- **Multiple Output Formats**: Text, UML, JSON, YAML, CSV
- **Relationship Detection**: Automatically identifies relationships between tables
- **BigQuery Type Support**: Handles BigQuery-specific types (ARRAY, STRUCT, TIMESTAMP, etc.)
- **Batch Processing**: Can process multiple files at once

## Limitations

- Requires manual export from BigQuery (no direct connection)
- Relationship detection is heuristic-based (naming conventions)
- Nested STRUCT types may need manual review

## Troubleshooting

**Issue**: "No tables found in input files"
- **Solution**: Verify your export files contain the expected columns (table_name, column_name, data_type)

**Issue**: "Invalid JSON format"
- **Solution**: Validate your JSON files using a JSON validator

**Issue**: "Relationship detection not working"
- **Solution**: Check that column names follow common patterns (e.g., `user_id` → `users.id`)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details.

