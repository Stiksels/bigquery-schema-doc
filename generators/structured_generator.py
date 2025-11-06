"""
Generate structured format outputs (JSON, YAML, CSV) from schema model.
"""
import csv
import json
import logging
from pathlib import Path
from typing import Dict, List, Any

import yaml

try:
    from ..schema_model import DatasetSchema, TableSchema, ColumnSchema, Relationship, ColumnMode
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from schema_model import DatasetSchema, TableSchema, ColumnSchema, Relationship, ColumnMode

logger = logging.getLogger(__name__)


def generate_json_schema(dataset: DatasetSchema, output_path: Path) -> None:
    """
    Generate JSON schema definition from dataset.
    
    Args:
        dataset: DatasetSchema object to export
        output_path: Path to output JSON file
    """
    logger.info(f"Generating JSON schema: {output_path}")
    
    schema_dict = _dataset_to_dict(dataset)
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(schema_dict, f, indent=2, ensure_ascii=False)
    
    logger.info(f"JSON schema written to: {output_path}")


def generate_yaml_schema(dataset: DatasetSchema, output_path: Path) -> None:
    """
    Generate YAML schema definition from dataset.
    
    Args:
        dataset: DatasetSchema object to export
        output_path: Path to output YAML file
    """
    logger.info(f"Generating YAML schema: {output_path}")
    
    schema_dict = _dataset_to_dict(dataset)
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        yaml.dump(schema_dict, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
    
    logger.info(f"YAML schema written to: {output_path}")


def generate_csv_export(dataset: DatasetSchema, output_path: Path) -> None:
    """
    Generate CSV export of schema information.
    
    Args:
        dataset: DatasetSchema object to export
        output_path: Path to output CSV file
    """
    logger.info(f"Generating CSV export: {output_path}")
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow([
            'table_name',
            'table_description',
            'column_name',
            'data_type',
            'mode',
            'column_description',
            'is_primary_key',
            'is_foreign_key',
            'foreign_key_table',
            'foreign_key_column'
        ])
        
        # Write rows
        for table_name in sorted(dataset.tables.keys()):
            table = dataset.tables[table_name]
            
            for column in sorted(table.columns, key=lambda c: c.name):
                is_pk = 'Yes' if (column.name == table.primary_key) else 'No'
                is_fk = 'Yes' if column.is_foreign_key else 'No'
                fk_table = column.foreign_key_table or ''
                fk_column = column.foreign_key_column or ''
                
                writer.writerow([
                    table_name,
                    table.description or '',
                    column.name,
                    column.data_type,
                    column.mode.value,
                    column.description or '',
                    is_pk,
                    is_fk,
                    fk_table,
                    fk_column
                ])
        
        # Write relationships section
        all_relationships = dataset.get_all_relationships()
        if all_relationships:
            writer.writerow([])  # Empty row separator
            writer.writerow(['Relationships'])
            writer.writerow([
                'from_table',
                'from_column',
                'to_table',
                'to_column',
                'relationship_type',
                'confidence'
            ])
            
            for rel in all_relationships:
                writer.writerow([
                    rel.from_table,
                    rel.from_column,
                    rel.to_table,
                    rel.to_column,
                    rel.relationship_type,
                    rel.confidence
                ])
    
    logger.info(f"CSV export written to: {output_path}")


def _dataset_to_dict(dataset: DatasetSchema) -> Dict[str, Any]:
    """Convert dataset schema to dictionary for JSON/YAML export."""
    schema_dict = {
        'dataset_name': dataset.name,
        'metadata': {
            'table_count': len(dataset.tables),
            'total_columns': sum(len(table.columns) for table in dataset.tables.values()),
            'total_relationships': len(dataset.get_all_relationships())
        },
        'tables': []
    }
    
    for table_name in sorted(dataset.tables.keys()):
        table = dataset.tables[table_name]
        table_dict = _table_to_dict(table)
        schema_dict['tables'].append(table_dict)
    
    # Add relationships
    all_relationships = dataset.get_all_relationships()
    if all_relationships:
        schema_dict['relationships'] = []
        for rel in all_relationships:
            schema_dict['relationships'].append({
                'from_table': rel.from_table,
                'from_column': rel.from_column,
                'to_table': rel.to_table,
                'to_column': rel.to_column,
                'relationship_type': rel.relationship_type,
                'confidence': rel.confidence
            })
    
    return schema_dict


def _table_to_dict(table: TableSchema) -> Dict[str, Any]:
    """Convert table schema to dictionary."""
    table_dict = {
        'name': table.name,
        'description': table.description,
        'primary_key': table.primary_key,
        'columns': []
    }
    
    for column in sorted(table.columns, key=lambda c: c.name):
        column_dict = {
            'name': column.name,
            'data_type': column.data_type,
            'mode': column.mode.value,
            'description': column.description,
            'is_primary_key': column.is_primary_key or (column.name == table.primary_key),
            'is_foreign_key': column.is_foreign_key
        }
        
        if column.is_foreign_key:
            column_dict['foreign_key'] = {
                'table': column.foreign_key_table,
                'column': column.foreign_key_column
            }
        
        table_dict['columns'].append(column_dict)
    
    # Add relationships specific to this table
    if table.relationships:
        table_dict['relationships'] = []
        for rel in table.relationships:
            table_dict['relationships'].append({
                'from_column': rel.from_column,
                'to_table': rel.to_table,
                'to_column': rel.to_column,
                'relationship_type': rel.relationship_type,
                'confidence': rel.confidence
            })
    
    return table_dict

