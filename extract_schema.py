"""
Extract schema information from manually exported BigQuery files.
"""
import json
import logging
import re
from pathlib import Path
from typing import List, Dict, Optional, Union

import pandas as pd

try:
    from .schema_model import (
        DatasetSchema,
        TableSchema,
        ColumnSchema,
        Relationship,
        ColumnMode,
    )
except ImportError:
    from schema_model import (
        DatasetSchema,
        TableSchema,
        ColumnSchema,
        Relationship,
        ColumnMode,
    )

logger = logging.getLogger(__name__)

# Tables to exclude from schema (BI views, aggregates, etc.)
EXCLUDED_TABLE_PATTERNS = [
    r'_view_bi',  # BI view tables (matches _view_bi anywhere in the name)
]


def load_excluded_tables_from_config(input_path: Path) -> List[str]:
    """
    Load excluded table names from a config file in the same directory as input.
    
    Looks for 'exclude_tables.txt' or 'schema_config.json' in the input file's directory.
    
    Args:
        input_path: Path to the input schema file
    
    Returns:
        List of table names to exclude
    """
    excluded = []
    
    # Try exclude_tables.txt (one table name per line)
    exclude_txt = input_path.parent / 'exclude_tables.txt'
    if exclude_txt.exists():
        try:
            with open(exclude_txt, 'r', encoding='utf-8') as f:
                for line in f:
                    table_name = line.strip()
                    if table_name and not table_name.startswith('#'):
                        excluded.append(table_name)
            logger.info(f"Loaded {len(excluded)} excluded tables from {exclude_txt}")
        except Exception as e:
            logger.warning(f"Failed to read exclude_tables.txt: {e}")
    
    # Try schema_config.json
    config_json = input_path.parent / 'schema_config.json'
    if config_json.exists():
        try:
            import json
            with open(config_json, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if 'exclude_tables' in config:
                    excluded.extend(config['exclude_tables'])
            logger.info(f"Loaded excluded tables from {config_json}")
        except Exception as e:
            logger.warning(f"Failed to read schema_config.json: {e}")
    
    return excluded


def load_included_tables_from_config(input_path: Path) -> List[str]:
    """
    Load included table names from a config file in the same directory as input.
    
    These tables will always be included in simplified schemas, regardless of relationship count.
    Looks for 'include_tables.txt' or 'schema_config.json' in the input file's directory.
    
    Args:
        input_path: Path to the input schema file
    
    Returns:
        List of table names to always include
    """
    included = []
    
    # Try include_tables.txt (one table name per line)
    include_txt = input_path.parent / 'include_tables.txt'
    if include_txt.exists():
        try:
            with open(include_txt, 'r', encoding='utf-8') as f:
                for line in f:
                    table_name = line.strip()
                    if table_name and not table_name.startswith('#'):
                        included.append(table_name)
            logger.info(f"Loaded {len(included)} always-included tables from {include_txt}")
        except Exception as e:
            logger.warning(f"Failed to read include_tables.txt: {e}")
    
    # Try schema_config.json
    config_json = input_path.parent / 'schema_config.json'
    if config_json.exists():
        try:
            import json
            with open(config_json, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if 'include_tables' in config:
                    included.extend(config['include_tables'])
            logger.info(f"Loaded included tables from {config_json}")
        except Exception as e:
            logger.warning(f"Failed to read schema_config.json: {e}")
    
    return included


def should_exclude_table(table_name: str, excluded_table_names: Optional[List[str]] = None) -> bool:
    """
    Check if a table should be excluded from the schema.
    
    Args:
        table_name: Name of the table to check
        excluded_table_names: Optional list of specific table names to exclude
    
    Returns:
        True if table should be excluded, False otherwise
    """
    # Check specific table names (exact match, case-insensitive)
    if excluded_table_names:
        table_name_lower = table_name.lower()
        for excluded_name in excluded_table_names:
            if table_name_lower == excluded_name.lower():
                return True
    
    # Check patterns
    for pattern in EXCLUDED_TABLE_PATTERNS:
        if re.search(pattern, table_name, re.IGNORECASE):
            return True
    return False


def parse_csv_schema(file_path: Path) -> DatasetSchema:
    """
    Parse schema from CSV export file.
    
    Expected CSV columns:
    - table_name (or table_id, table_catalog.table_schema.table_name)
    - column_name
    - data_type
    - mode (NULLABLE, REQUIRED, REPEATED)
    - description (optional)
    - table_description (optional)
    """
    logger.info(f"Parsing CSV file: {file_path}")
    
    # Load product-specific excluded tables from config
    excluded_table_names = load_excluded_tables_from_config(file_path)
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Failed to read CSV file: {e}")
        raise
    
    # Normalize column names (handle variations)
    column_mapping = {}
    for col in df.columns:
        col_lower = col.lower()
        if 'table' in col_lower and 'name' in col_lower:
            if 'table_id' in col_lower or 'table_catalog' in col_lower:
                column_mapping[col] = 'table_name'
            else:
                column_mapping[col] = 'table_name'
        elif 'column' in col_lower and 'name' in col_lower:
            column_mapping[col] = 'column_name'
        elif 'data_type' in col_lower or 'type' in col_lower:
            column_mapping[col] = 'data_type'
        elif 'mode' in col_lower:
            column_mapping[col] = 'mode'
        elif 'description' in col_lower and 'table' not in col_lower:
            column_mapping[col] = 'description'
        elif 'table_description' in col_lower or ('description' in col_lower and 'table' in col_lower):
            column_mapping[col] = 'table_description'
    
    # Rename columns
    df = df.rename(columns=column_mapping)
    
    # Check required columns
    required_cols = ['table_name', 'column_name', 'data_type']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Extract table name from full table identifier if needed
    if 'table_name' in df.columns:
        df['table_name'] = df['table_name'].apply(_extract_table_name)
    
    dataset = DatasetSchema()
    tables: Dict[str, TableSchema] = {}
    
    # Group by table
    for table_name, group in df.groupby('table_name'):
        table_name = str(table_name).strip()
        if not table_name:
            continue
        
        # Skip excluded tables (e.g., BI views)
        if should_exclude_table(table_name, excluded_table_names):
            logger.debug(f"Skipping excluded table: {table_name}")
            continue
        
        # Get table description (should be same for all rows of same table)
        table_description = None
        if 'table_description' in group.columns:
            table_desc_series = group['table_description'].dropna()
            if not table_desc_series.empty:
                table_description = str(table_desc_series.iloc[0]).strip()
        
        table = TableSchema(name=table_name, description=table_description)
        
        # Process columns
        for _, row in group.iterrows():
            column_name = str(row['column_name']).strip()
            if not column_name:
                continue
            
            data_type = str(row['data_type']).strip()
            
            # Parse mode
            mode = ColumnMode.NULLABLE
            if 'mode' in row and pd.notna(row['mode']):
                mode_str = str(row['mode']).strip().upper()
                if mode_str == 'REQUIRED':
                    mode = ColumnMode.REQUIRED
                elif mode_str == 'REPEATED':
                    mode = ColumnMode.REPEATED
            
            # Get description
            description = None
            if 'description' in row and pd.notna(row['description']):
                description = str(row['description']).strip()
            
            column = ColumnSchema(
                name=column_name,
                data_type=data_type,
                mode=mode,
                description=description
            )
            
            table.add_column(column)
        
        tables[table_name] = table
        dataset.add_table(table)
    
    logger.info(f"Parsed {len(tables)} tables from CSV")
    return dataset


def parse_json_schema(file_path: Path) -> DatasetSchema:
    """
    Parse schema from JSON export file.
    
    Supports multiple JSON formats:
    1. Array of table objects with columns
    2. Single table object
    3. BigQuery schema format (array of column definitions)
    """
    logger.info(f"Parsing JSON file: {file_path}")
    
    # Load product-specific excluded tables from config
    excluded_table_names = load_excluded_tables_from_config(file_path)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read JSON file: {e}")
        raise
    
    dataset = DatasetSchema()
    tables: Dict[str, TableSchema] = {}
    
    # Handle different JSON structures
    if isinstance(data, list):
        # Array of tables or columns
        if len(data) > 0:
            # Check if first element has 'columns' (table format) or 'name' (column format)
            first_item = data[0]
            if isinstance(first_item, dict) and 'columns' in first_item:
                # Format: Array of table objects
                for table_data in data:
                    table = _parse_table_from_json(table_data)
                    if table and not should_exclude_table(table.name, excluded_table_names):
                        tables[table.name] = table
                        dataset.add_table(table)
            elif isinstance(first_item, dict) and 'name' in first_item:
                # Format: BigQuery schema format (array of columns)
                # Need table name from filename or default
                table_name = _extract_table_name_from_path(file_path)
                table = _parse_bigquery_schema_format(data, table_name)
                if table and not should_exclude_table(table.name, excluded_table_names):
                    tables[table.name] = table
                    dataset.add_table(table)
    elif isinstance(data, dict):
        # Single table object or dataset object
        if 'columns' in data:
            # Single table
            table = _parse_table_from_json(data)
            if table and not should_exclude_table(table.name, excluded_table_names):
                tables[table.name] = table
                dataset.add_table(table)
        elif 'tables' in data:
            # Dataset with multiple tables
            for table_data in data['tables']:
                table = _parse_table_from_json(table_data)
                if table and not should_exclude_table(table.name, excluded_table_names):
                    tables[table.name] = table
                    dataset.add_table(table)
    
    logger.info(f"Parsed {len(tables)} tables from JSON")
    return dataset


def _parse_table_from_json(table_data: Dict) -> Optional[TableSchema]:
    """Parse a table object from JSON."""
    table_name = table_data.get('table_name') or table_data.get('name', 'unknown_table')
    table_description = table_data.get('table_description') or table_data.get('description')
    
    table = TableSchema(name=str(table_name), description=table_description)
    
    columns = table_data.get('columns', [])
    if not columns:
        return None
    
    for col_data in columns:
        if isinstance(col_data, dict):
            column = _parse_column_from_json(col_data)
            if column:
                table.add_column(column)
    
    return table


def _parse_column_from_json(col_data: Dict) -> Optional[ColumnSchema]:
    """Parse a column object from JSON."""
    name = col_data.get('name') or col_data.get('column_name')
    if not name:
        return None
    
    data_type = col_data.get('type') or col_data.get('data_type', 'STRING')
    
    # Parse mode
    mode = ColumnMode.NULLABLE
    mode_str = col_data.get('mode', '').upper()
    if mode_str == 'REQUIRED':
        mode = ColumnMode.REQUIRED
    elif mode_str == 'REPEATED':
        mode = ColumnMode.REPEATED
    
    description = col_data.get('description')
    
    return ColumnSchema(
        name=str(name),
        data_type=str(data_type),
        mode=mode,
        description=description
    )


def _parse_bigquery_schema_format(columns: List[Dict], table_name: str) -> Optional[TableSchema]:
    """Parse BigQuery schema format (array of column definitions)."""
    table = TableSchema(name=table_name)
    
    for col_data in columns:
        column = _parse_column_from_json(col_data)
        if column:
            table.add_column(column)
    
    return table if table.columns else None


def _extract_table_name(table_identifier: str) -> str:
    """Extract table name from full identifier like 'project.dataset.table'."""
    parts = str(table_identifier).split('.')
    return parts[-1] if parts else table_identifier


def _extract_table_name_from_path(file_path: Path) -> str:
    """Extract table name from file path."""
    name = file_path.stem
    # Remove common suffixes
    name = re.sub(r'_schema$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'_export$', '', name, flags=re.IGNORECASE)
    return name


def detect_relationships(dataset: DatasetSchema) -> None:
    """
    Detect relationships between tables based on column naming patterns.
    
    Common patterns:
    - user_id -> users.id
    - order_id -> orders.id
    - foreign_key_table_id -> foreign_key_table.id
    - locationId -> places.id (special case)
    """
    logger.info("Detecting relationships between tables")
    
    table_names = set(dataset.tables.keys())
    
    # Special mappings for common patterns
    special_mappings = {
        'locationid': 'places',
        'location_id': 'places',
        'placeid': 'places',
        'place_id': 'places',
    }
    
    for table_name, table in dataset.tables.items():
        for column in table.columns:
            # Pattern: column_name ends with '_id' and matches a table name
            column_name_lower = column.name.lower()
            
            # Check special mappings first
            if column_name_lower in special_mappings:
                target_table = special_mappings[column_name_lower]
                if target_table in table_names:
                    relationship = Relationship(
                        from_table=table_name,
                        from_column=column.name,
                        to_table=target_table,
                        to_column='id',
                        relationship_type='foreign_key',
                        confidence=1.0
                    )
                    table.add_relationship(relationship)
                    column.is_foreign_key = True
                    column.foreign_key_table = target_table
                    column.foreign_key_column = 'id'
                    continue
            
            # Check for common foreign key patterns
            patterns = [
                (r'^(.+)_id$', r'\1'),  # user_id -> user
                (r'^(.+)_uuid$', r'\1'),  # user_uuid -> user
                (r'^(.+)_key$', r'\1'),  # user_key -> user
                (r'^(.+?)id$', r'\1'),  # organizerId -> organizer (camelCase)
            ]
            
            for pattern, replacement in patterns:
                match = re.match(pattern, column_name_lower)
                if match:
                    potential_table = match.group(1)
                    
                    # Try exact match first
                    if potential_table in table_names:
                        relationship = Relationship(
                            from_table=table_name,
                            from_column=column.name,
                            to_table=potential_table,
                            to_column='id',  # Assume primary key is 'id'
                            relationship_type='foreign_key',
                            confidence=0.9
                        )
                        table.add_relationship(relationship)
                        column.is_foreign_key = True
                        column.foreign_key_table = potential_table
                        column.foreign_key_column = 'id'
                        break
                    
                    # Try plural/singular variations
                    # Remove 's' from end (users -> user)
                    if potential_table.endswith('s') and potential_table[:-1] in table_names:
                        relationship = Relationship(
                            from_table=table_name,
                            from_column=column.name,
                            to_table=potential_table[:-1],
                            to_column='id',
                            relationship_type='foreign_key',
                            confidence=0.7
                        )
                        table.add_relationship(relationship)
                        column.is_foreign_key = True
                        column.foreign_key_table = potential_table[:-1]
                        column.foreign_key_column = 'id'
                        break
                    
                    # Add 's' to end (user -> users, organizer -> organizers)
                    plural = potential_table + 's'
                    if plural in table_names:
                        relationship = Relationship(
                            from_table=table_name,
                            from_column=column.name,
                            to_table=plural,
                            to_column='id',
                            relationship_type='foreign_key',
                            confidence=0.7
                        )
                        table.add_relationship(relationship)
                        column.is_foreign_key = True
                        column.foreign_key_table = plural
                        column.foreign_key_column = 'id'
                        break


def parse_table_schema_from_file(file_path: Path, file_format: Optional[str] = None) -> DatasetSchema:
    """
    Parse schema from exported file, auto-detecting format if not specified.
    
    Args:
        file_path: Path to the exported schema file
        file_format: Optional format ('csv' or 'json'), auto-detected if None
    
    Returns:
        DatasetSchema object containing parsed schema
    """
    if file_format is None:
        # Auto-detect format
        if file_path.suffix.lower() == '.csv':
            file_format = 'csv'
        elif file_path.suffix.lower() == '.json':
            file_format = 'json'
        else:
            raise ValueError(f"Cannot auto-detect format for file: {file_path}")
    
    if file_format.lower() == 'csv':
        return parse_csv_schema(file_path)
    elif file_format.lower() == 'json':
        return parse_json_schema(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def build_schema_model(input_files: Union[Path, List[Path]], file_format: Optional[str] = None) -> DatasetSchema:
    """
    Build complete schema model from one or more exported files.
    
    Args:
        input_files: Single file path or list of file paths
        file_format: Optional format ('csv' or 'json'), auto-detected if None
    
    Returns:
        Combined DatasetSchema object
    """
    if isinstance(input_files, Path) or isinstance(input_files, str):
        input_files = [Path(input_files)]
    else:
        input_files = [Path(f) for f in input_files]
    
    # Combine multiple files into one dataset
    combined_dataset = DatasetSchema()
    
    for file_path in input_files:
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            continue
        
        dataset = parse_table_schema_from_file(file_path, file_format)
        
        # Merge tables
        for table in dataset.tables.values():
            if table.name in combined_dataset.tables:
                # Merge columns if table already exists
                existing_table = combined_dataset.tables[table.name]
                for col in table.columns:
                    if col.name not in [c.name for c in existing_table.columns]:
                        existing_table.add_column(col)
            else:
                combined_dataset.add_table(table)
    
    # Detect relationships across all tables
    detect_relationships(combined_dataset)
    
    return combined_dataset

