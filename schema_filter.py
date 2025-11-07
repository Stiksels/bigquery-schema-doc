"""
Schema filtering module for creating simplified schemas.
"""
import logging
import re
from typing import Dict, List, Set, Optional
from dataclasses import dataclass

from schema_model import DatasetSchema, TableSchema, Relationship

logger = logging.getLogger(__name__)


@dataclass
class FilterConfig:
    """Configuration for schema filtering."""
    min_relationships: int = 2
    include_tables: Optional[List[str]] = None
    exclude_patterns: Optional[List[str]] = None
    include_patterns: Optional[List[str]] = None
    top_n: Optional[int] = None
    include_connected: bool = True  # Include tables connected to seed tables


def filter_by_relationship_count(dataset: DatasetSchema, min_relationships: int) -> Set[str]:
    """
    Filter tables by minimum relationship count.
    
    Args:
        dataset: DatasetSchema to filter
        min_relationships: Minimum number of relationships required
    
    Returns:
        Set of table names that meet the criteria
    """
    from schema_analyzer import identify_core_entities
    return identify_core_entities(dataset, min_relationships)


def filter_by_patterns(
    dataset: DatasetSchema,
    include_patterns: Optional[List[str]] = None,
    exclude_patterns: Optional[List[str]] = None
) -> Set[str]:
    """
    Filter tables by name patterns.
    
    Args:
        dataset: DatasetSchema to filter
        include_patterns: List of glob patterns to include (e.g., ['*entity*', 'passholder*'])
        exclude_patterns: List of glob patterns to exclude (e.g., ['*_view_bi', '*_dump'])
    
    Returns:
        Set of table names that match the patterns
    """
    logger.info("Filtering tables by patterns")
    
    selected_tables = set()
    all_tables = set(dataset.tables.keys())
    
    # Convert glob patterns to regex
    def glob_to_regex(pattern: str) -> str:
        pattern = pattern.replace('.', r'\.')
        pattern = pattern.replace('*', '.*')
        pattern = pattern.replace('?', '.')
        return f'^{pattern}$'
    
    # Apply include patterns
    if include_patterns:
        for pattern in include_patterns:
            regex = glob_to_regex(pattern)
            for table_name in all_tables:
                if re.match(regex, table_name, re.IGNORECASE):
                    selected_tables.add(table_name)
    else:
        # If no include patterns, start with all tables
        selected_tables = all_tables.copy()
    
    # Apply exclude patterns
    if exclude_patterns:
        for pattern in exclude_patterns:
            regex = glob_to_regex(pattern)
            for table_name in list(selected_tables):
                if re.match(regex, table_name, re.IGNORECASE):
                    selected_tables.remove(table_name)
    
    logger.info(f"Pattern filtering: {len(selected_tables)} tables selected")
    return selected_tables


def filter_by_importance(
    dataset: DatasetSchema,
    include_tables: List[str],
    min_relationships: int = 1
) -> Set[str]:
    """
    Include specific tables plus their connected neighbors.
    
    Args:
        dataset: DatasetSchema to filter
        include_tables: List of table names to explicitly include
        min_relationships: Minimum relationships for connected neighbors
    
    Returns:
        Set of table names including seed tables and their connections
    """
    from schema_analyzer import get_connected_subgraph, identify_core_entities
    
    logger.info(f"Including {len(include_tables)} seed tables and their connections")
    
    seed_set = set(include_tables)
    
    # Get all connected tables
    if seed_set:
        connected = get_connected_subgraph(dataset, seed_set)
        
        # Also include core entities that meet minimum relationship threshold
        core_entities = identify_core_entities(dataset, min_relationships)
        
        # Combine: seed tables + connected tables + core entities
        result = seed_set | connected | core_entities
    else:
        result = identify_core_entities(dataset, min_relationships)
    
    logger.info(f"Importance filtering: {len(result)} tables selected")
    return result


def create_simplified_schema(dataset: DatasetSchema, filter_config: FilterConfig) -> DatasetSchema:
    """
    Create a simplified schema based on filter configuration.
    
    Args:
        dataset: Original DatasetSchema
        filter_config: FilterConfig with filtering criteria
    
    Returns:
        New DatasetSchema containing only selected tables and their relationships
    """
    logger.info("Creating simplified schema")
    
    selected_tables = set()
    
    # Strategy 0: Always include explicitly named tables (from config or CLI)
    # These are added first so they're always included regardless of other filters
    if filter_config.include_tables:
        include_set = set(filter_config.include_tables)
        # Only include tables that actually exist in the dataset
        existing_include_set = {t for t in include_set if t in dataset.tables}
        selected_tables.update(existing_include_set)
        if len(existing_include_set) < len(include_set):
            missing = include_set - existing_include_set
            logger.warning(f"Some requested tables not found in dataset: {missing}")
        logger.info(f"Always-included tables: {len(existing_include_set)} tables")
    
    # Strategy 1: Include by relationship count
    if filter_config.min_relationships > 0:
        by_count = filter_by_relationship_count(dataset, filter_config.min_relationships)
        selected_tables.update(by_count)
        logger.info(f"Relationship count filter: {len(by_count)} tables")
    
    # Strategy 3: Filter by patterns
    pattern_filtered = filter_by_patterns(
        dataset,
        filter_config.include_patterns,
        filter_config.exclude_patterns
    )
    
    # If patterns are specified, intersect with pattern results
    if filter_config.include_patterns or filter_config.exclude_patterns:
        selected_tables = selected_tables & pattern_filtered
        logger.info(f"Pattern filter: {len(pattern_filtered)} tables match patterns")
    
    # Strategy 4: Top N tables
    if filter_config.top_n:
        from schema_analyzer import get_top_tables_by_relationships
        top_tables = get_top_tables_by_relationships(dataset, filter_config.top_n)
        selected_tables.update(top_tables)
        logger.info(f"Top N filter: {len(top_tables)} tables")
    
    # If no filters applied, default to relationship count
    if not selected_tables and filter_config.min_relationships > 0:
        selected_tables = filter_by_relationship_count(dataset, filter_config.min_relationships)
    
    # Create simplified dataset
    simplified = DatasetSchema(name=dataset.name)
    
    # Add selected tables (create copies to avoid modifying originals)
    for table_name in selected_tables:
        if table_name in dataset.tables:
            original_table = dataset.tables[table_name]
            # Create a new table with same data but empty relationships
            from schema_model import TableSchema, ColumnSchema
            new_table = TableSchema(
                name=original_table.name,
                columns=original_table.columns.copy(),
                description=original_table.description,
                primary_key=original_table.primary_key,
                relationships=[]  # Start with empty relationships
            )
            simplified.add_table(new_table)
    
    # Filter relationships to only include those between selected tables
    # Deduplicate as we go
    seen_relationships = set()
    all_relationships = dataset.get_all_relationships()
    for rel in all_relationships:
        if rel.from_table in selected_tables and rel.to_table in selected_tables:
            # Create a key for deduplication
            rel_key = (rel.from_table, rel.from_column, rel.to_table, rel.to_column)
            if rel_key not in seen_relationships:
                seen_relationships.add(rel_key)
                simplified.add_relationship(rel)
                # Also add to the from_table's relationships
                if rel.from_table in simplified.tables:
                    simplified.tables[rel.from_table].add_relationship(rel)
    
    stats = simplified.get_statistics()
    original_stats = dataset.get_statistics()
    
    logger.info(
        f"Simplified schema: {stats['table_count']} tables "
        f"(reduced from {original_stats['table_count']}), "
        f"{stats['relationship_count']} relationships "
        f"(from {original_stats['relationship_count']})"
    )
    
    return simplified

