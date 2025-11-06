"""
Generate UML diagrams from schema model (PlantUML and Mermaid formats).
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


def generate_plantuml_diagram(dataset: DatasetSchema, output_path: Path) -> None:
    """
    Generate PlantUML class diagram from dataset schema.
    
    Args:
        dataset: DatasetSchema object to diagram
        output_path: Path to output PlantUML file
    """
    logger.info(f"Generating PlantUML diagram: {output_path}")
    
    lines = []
    
    # Header
    dataset_name = dataset.name or "BigQuery Dataset"
    lines.append("@startuml")
    lines.append(f"title {dataset_name} Schema Diagram")
    lines.append("")
    
    # Generate class for each table
    for table_name in sorted(dataset.tables.keys()):
        table = dataset.tables[table_name]
        lines.extend(_generate_plantuml_class(table))
        lines.append("")
    
    # Generate relationships
    all_relationships = dataset.get_all_relationships()
    if all_relationships:
        lines.append("' Relationships")
        for rel in all_relationships:
            # Use PlantUML arrow syntax
            arrow = "-->" if rel.relationship_type == "foreign_key" else "--"
            lines.append(f"{rel.from_table} {arrow} {rel.to_table} : {rel.from_column}")
        lines.append("")
    
    lines.append("@enduml")
    
    # Write to file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    logger.info(f"PlantUML diagram written to: {output_path}")


def generate_mermaid_diagram(dataset: DatasetSchema, output_path: Path) -> None:
    """
    Generate Mermaid ER diagram from dataset schema.
    
    Args:
        dataset: DatasetSchema object to diagram
        output_path: Path to output Mermaid file
    """
    logger.info(f"Generating Mermaid diagram: {output_path}")
    
    lines = []
    
    # Header
    dataset_name = dataset.name or "BigQuery Dataset"
    lines.append("erDiagram")
    lines.append("")
    
    # Generate entity for each table
    for table_name in sorted(dataset.tables.keys()):
        table = dataset.tables[table_name]
        lines.extend(_generate_mermaid_entity(table))
        lines.append("")
    
    # Generate relationships
    all_relationships = dataset.get_all_relationships()
    if all_relationships:
        lines.append("    %% Relationships")
        for rel in all_relationships:
            # Mermaid relationship syntax: Entity1 ||--o{ Entity2 : "relationship"
            # Format: Entity1 ||--o| Entity2 : "relationship_description"
            rel_desc = f"{rel.from_column} -> {rel.to_column}"
            lines.append(f'    {rel.from_table} ||--o{{ {rel.to_table} : "{rel_desc}"')
        lines.append("")
    
    # Write to file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    logger.info(f"Mermaid diagram written to: {output_path}")


def _generate_plantuml_class(table: TableSchema) -> list:
    """Generate PlantUML class definition for a table."""
    lines = []
    
    lines.append(f"class {table.name} {{")
    
    # Add description as note
    if table.description:
        # Escape special characters for PlantUML
        desc = table.description.replace('"', '\\"')
        lines.append(f"  note top : {desc}")
        lines.append("")
    
    # Primary key
    if table.primary_key:
        lines.append(f"  ** Primary Key: {table.primary_key} **")
        lines.append("")
    
    # Columns
    for column in sorted(table.columns, key=lambda c: c.name):
        # Format: +name: type (mode)
        visibility = "+"  # Public in UML
        
        # Add mode indicator
        mode_str = ""
        if column.mode == ColumnMode.REQUIRED:
            mode_str = " [required]"
        elif column.mode == ColumnMode.REPEATED:
            mode_str = " [repeated]"
        
        # Add foreign key indicator
        fk_str = ""
        if column.is_foreign_key:
            fk_str = f" -> {column.foreign_key_table}"
        
        column_line = f"  {visibility}{column.name}: {column.data_type}{mode_str}{fk_str}"
        lines.append(column_line)
    
    lines.append("}")
    
    return lines


def _generate_mermaid_entity(table: TableSchema) -> list:
    """Generate Mermaid entity definition for a table."""
    lines = []
    
    # Mermaid entity syntax:
    # EntityName {
    #   type field_name
    # }
    # Note: Mermaid ER diagrams don't support inline modifiers like PK/FK
    # These are better represented in the relationship definitions
    
    lines.append(f"    {table.name} {{")
    
    for column in sorted(table.columns, key=lambda c: c.name):
        # Format: type field_name
        # Mermaid ER diagrams don't support spaces in attribute names
        # Replace spaces with underscores for compatibility
        field_name = column.name.replace(' ', '_')
        field_line = f" {column.data_type} {field_name}"
        lines.append(field_line)
    
    lines.append("    }")
    
    return lines

