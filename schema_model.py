"""
Data model classes for representing BigQuery schema information.
"""
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from enum import Enum


class ColumnMode(Enum):
    """Column mode types in BigQuery."""
    NULLABLE = "NULLABLE"
    REQUIRED = "REQUIRED"
    REPEATED = "REPEATED"


@dataclass
class ColumnSchema:
    """Schema definition for a single column."""
    name: str
    data_type: str
    mode: ColumnMode = ColumnMode.NULLABLE
    description: Optional[str] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_table: Optional[str] = None
    foreign_key_column: Optional[str] = None
    
    def __str__(self) -> str:
        """String representation of column."""
        mode_str = f" {self.mode.value}" if self.mode != ColumnMode.NULLABLE else ""
        desc_str = f" - {self.description}" if self.description else ""
        return f"{self.name}: {self.data_type}{mode_str}{desc_str}"


@dataclass
class Relationship:
    """Represents a relationship between two tables."""
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    relationship_type: str = "foreign_key"  # foreign_key, one_to_many, many_to_one, etc.
    confidence: float = 1.0  # Confidence score for inferred relationships
    
    def __str__(self) -> str:
        """String representation of relationship."""
        return f"{self.from_table}.{self.from_column} -> {self.to_table}.{self.to_column}"


@dataclass
class TableSchema:
    """Schema definition for a table."""
    name: str
    columns: List[ColumnSchema] = field(default_factory=list)
    description: Optional[str] = None
    primary_key: Optional[str] = None
    relationships: List[Relationship] = field(default_factory=list)
    
    def get_column(self, column_name: str) -> Optional[ColumnSchema]:
        """Get a column by name."""
        for col in self.columns:
            if col.name == column_name:
                return col
        return None
    
    def add_column(self, column: ColumnSchema) -> None:
        """Add a column to the table."""
        self.columns.append(column)
    
    def add_relationship(self, relationship: Relationship) -> None:
        """Add a relationship to the table."""
        self.relationships.append(relationship)
    
    def __str__(self) -> str:
        """String representation of table."""
        desc_str = f" - {self.description}" if self.description else ""
        return f"Table: {self.name}{desc_str} ({len(self.columns)} columns)"


@dataclass
class DatasetSchema:
    """Complete dataset schema model containing all tables."""
    name: Optional[str] = None
    tables: Dict[str, TableSchema] = field(default_factory=dict)
    relationships: List[Relationship] = field(default_factory=list)
    
    def add_table(self, table: TableSchema) -> None:
        """Add a table to the dataset."""
        self.tables[table.name] = table
    
    def get_table(self, table_name: str) -> Optional[TableSchema]:
        """Get a table by name."""
        return self.tables.get(table_name)
    
    def add_relationship(self, relationship: Relationship) -> None:
        """Add a dataset-level relationship."""
        self.relationships.append(relationship)
    
    def get_all_relationships(self) -> List[Relationship]:
        """Get all relationships from tables and dataset level."""
        all_relationships = list(self.relationships)
        for table in self.tables.values():
            all_relationships.extend(table.relationships)
        return all_relationships
    
    def get_statistics(self) -> Dict:
        """Get statistics about the dataset."""
        total_columns = sum(len(table.columns) for table in self.tables.values())
        total_relationships = len(self.get_all_relationships())
        
        return {
            "table_count": len(self.tables),
            "column_count": total_columns,
            "relationship_count": total_relationships,
            "tables": list(self.tables.keys())
        }
    
    def __str__(self) -> str:
        """String representation of dataset."""
        stats = self.get_statistics()
        name_str = f"Dataset: {self.name}\n" if self.name else ""
        return f"{name_str}Tables: {stats['table_count']}, Columns: {stats['column_count']}, Relationships: {stats['relationship_count']}"

