"""
Generate Markdown documentation from schema model.
"""
import logging
from pathlib import Path
from typing import Optional

try:
    from ..schema_model import DatasetSchema, TableSchema, ColumnSchema, Relationship, ColumnMode
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from schema_model import DatasetSchema, TableSchema, ColumnSchema, Relationship, ColumnMode

logger = logging.getLogger(__name__)


def generate_text_documentation(dataset: DatasetSchema, output_path: Path) -> None:
    """
    Generate Markdown documentation from dataset schema.
    
    Args:
        dataset: DatasetSchema object to document
        output_path: Path to output Markdown file
    """
    logger.info(f"Generating text documentation: {output_path}")
    
    lines = []
    
    # Title
    dataset_name = dataset.name or "BigQuery Dataset"
    lines.append(f"# {dataset_name} Schema Documentation\n")
    lines.append(f"*Generated schema documentation for semantic mapping workshop*\n")
    
    # Overview
    stats = dataset.get_statistics()
    lines.append("## Overview\n")
    lines.append(f"This dataset contains **{stats['table_count']}** tables with **{stats['column_count']}** columns.")
    if stats['relationship_count'] > 0:
        lines.append(f"**{stats['relationship_count']}** relationships have been detected between tables.\n")
    else:
        lines.append("\n")
    
    # Table of Contents
    lines.append("## Table of Contents\n")
    lines.append("- [Overview](#overview)")
    lines.append("- [Index](#index)")
    lines.append("- [Relationships](#relationships)")
    lines.append("- [Tables](#tables)")
    for table_name in sorted(dataset.tables.keys()):
        # Generate anchor from table name (lowercase, spaces and underscores to hyphens)
        anchor = table_name.lower().replace(' ', '-').replace('_', '-')
        lines.append(f"  - [{table_name}](#{anchor})")
    lines.append("")
    
    # Index Section
    lines.append("## Index\n")
    lines.append("### Tables\n")
    for table_name in sorted(dataset.tables.keys()):
        table = dataset.tables[table_name]
        column_count = len(table.columns)
        desc = f" - {table.description}" if table.description else ""
        lines.append(f"- **{table_name}** ({column_count} columns){desc}")
    lines.append("")
    
    # Relationships Section
    all_relationships = dataset.get_all_relationships()
    if all_relationships:
        # Deduplicate relationships based on (from_table, from_column, to_table, to_column)
        seen = set()
        unique_relationships = []
        for rel in all_relationships:
            key = (rel.from_table, rel.from_column, rel.to_table, rel.to_column)
            if key not in seen:
                seen.add(key)
                unique_relationships.append(rel)
        
        lines.append("## Relationships\n")
        lines.append("The following relationships have been detected between tables:\n")
        lines.append("| From Table | From Column | To Table | To Column | Type | Confidence |")
        lines.append("|------------|-------------|----------|-----------|------|------------|")
        
        for rel in unique_relationships:
            confidence_str = f"{rel.confidence:.1f}" if rel.confidence < 1.0 else "High"
            lines.append(
                f"| {rel.from_table} | {rel.from_column} | {rel.to_table} | {rel.to_column} | "
                f"{rel.relationship_type} | {confidence_str} |"
            )
        lines.append("")
    
    # Tables Section (moved to end)
    lines.append("## Tables\n")
    
    for table_name in sorted(dataset.tables.keys()):
        table = dataset.tables[table_name]
        lines.extend(_generate_table_section(table, dataset))
    
    # Write to file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    logger.info(f"Text documentation written to: {output_path}")


def _generate_table_section(table: TableSchema, dataset: DatasetSchema) -> list:
    """Generate documentation section for a single table."""
    lines = []
    
    # Table header (anchor is auto-generated from heading in most Markdown processors)
    lines.append(f"### {table.name}\n")
    
    # Table description
    if table.description:
        lines.append(f"{table.description}\n")
    
    # Table metadata
    lines.append(f"**Columns:** {len(table.columns)}")
    if table.primary_key:
        lines.append(f" | **Primary Key:** {table.primary_key}")
    lines.append("\n")
    
    # Columns table
    lines.append("| Column Name | Data Type | Mode | Description |")
    lines.append("|------------|----------|------|-------------|")
    
    for column in sorted(table.columns, key=lambda c: c.name):
        mode_str = column.mode.value
        desc_str = column.description or ""
        # Escape pipe characters in description
        desc_str = desc_str.replace('|', '\\|')
        
        # Mark foreign keys
        name_str = column.name
        if column.is_foreign_key:
            name_str = f"{column.name} → {column.foreign_key_table}"
        
        lines.append(f"| {name_str} | {column.data_type} | {mode_str} | {desc_str} |")
    
    lines.append("")
    
    # Relationships
    if table.relationships:
        lines.append("#### Relationships\n")
        for rel in table.relationships:
            lines.append(f"- `{rel.from_column}` references `{rel.to_table}.{rel.to_column}`")
        lines.append("")
    
    return lines

