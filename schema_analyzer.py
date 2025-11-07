"""
Schema analysis module for calculating relationship metrics and identifying core entities.
"""
import logging
from typing import Dict, List, Set, Tuple
from collections import defaultdict

from schema_model import DatasetSchema, TableSchema, Relationship

logger = logging.getLogger(__name__)


def calculate_relationship_counts(dataset: DatasetSchema) -> Dict[str, Dict[str, int]]:
    """
    Calculate relationship counts for each table.
    
    Returns:
        Dictionary mapping table names to counts:
        {
            'table_name': {
                'total': total_relationships,
                'incoming': incoming_relationships,
                'outgoing': outgoing_relationships
            }
        }
    """
    logger.info("Calculating relationship counts")
    
    all_relationships = dataset.get_all_relationships()
    
    counts = defaultdict(lambda: {'total': 0, 'incoming': 0, 'outgoing': 0})
    
    for rel in all_relationships:
        from_table = rel.from_table
        to_table = rel.to_table
        
        # Count outgoing relationship
        counts[from_table]['outgoing'] += 1
        counts[from_table]['total'] += 1
        
        # Count incoming relationship
        counts[to_table]['incoming'] += 1
        counts[to_table]['total'] += 1
    
    # Ensure all tables are in the result (even with 0 relationships)
    for table_name in dataset.tables.keys():
        if table_name not in counts:
            counts[table_name] = {'total': 0, 'incoming': 0, 'outgoing': 0}
    
    return dict(counts)


def calculate_centrality_scores(dataset: DatasetSchema) -> Dict[str, float]:
    """
    Calculate simple centrality scores based on relationship counts.
    
    Uses degree centrality (number of connections).
    
    Returns:
        Dictionary mapping table names to centrality scores (0.0 to 1.0)
    """
    logger.info("Calculating centrality scores")
    
    counts = calculate_relationship_counts(dataset)
    
    if not counts:
        return {}
    
    # Find maximum total relationships for normalization
    max_relationships = max(count['total'] for count in counts.values()) if counts else 1
    
    centrality = {}
    for table_name, count_data in counts.items():
        # Normalize to 0.0-1.0 range
        score = count_data['total'] / max_relationships if max_relationships > 0 else 0.0
        centrality[table_name] = score
    
    return centrality


def identify_core_entities(dataset: DatasetSchema, min_relationships: int = 2) -> Set[str]:
    """
    Identify core entities (tables) with at least min_relationships connections.
    
    Args:
        dataset: DatasetSchema to analyze
        min_relationships: Minimum number of relationships required
    
    Returns:
        Set of table names that meet the criteria
    """
    logger.info(f"Identifying core entities with minimum {min_relationships} relationships")
    
    counts = calculate_relationship_counts(dataset)
    core_entities = set()
    
    for table_name, count_data in counts.items():
        if count_data['total'] >= min_relationships:
            core_entities.add(table_name)
    
    logger.info(f"Found {len(core_entities)} core entities out of {len(dataset.tables)} total tables")
    return core_entities


def get_connected_subgraph(dataset: DatasetSchema, seed_tables: Set[str]) -> Set[str]:
    """
    Get all tables connected to seed tables (transitive closure).
    
    Starting from seed tables, includes all tables that are connected
    through relationships, recursively.
    
    Args:
        dataset: DatasetSchema to analyze
        seed_tables: Set of table names to start from
    
    Returns:
        Set of all connected table names
    """
    logger.info(f"Finding connected subgraph starting from {len(seed_tables)} seed tables")
    
    all_relationships = dataset.get_all_relationships()
    
    # Build adjacency list
    adjacency = defaultdict(set)
    for rel in all_relationships:
        adjacency[rel.from_table].add(rel.to_table)
        adjacency[rel.to_table].add(rel.from_table)  # Undirected for connectivity
    
    # BFS to find all connected tables
    visited = set(seed_tables)
    queue = list(seed_tables)
    
    while queue:
        current = queue.pop(0)
        
        for neighbor in adjacency.get(current, set()):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
    
    logger.info(f"Found {len(visited)} connected tables")
    return visited


def get_top_tables_by_relationships(dataset: DatasetSchema, top_n: int) -> Set[str]:
    """
    Get top N tables by total relationship count.
    
    Args:
        dataset: DatasetSchema to analyze
        top_n: Number of top tables to return
    
    Returns:
        Set of table names (top N by relationship count)
    """
    logger.info(f"Getting top {top_n} tables by relationship count")
    
    counts = calculate_relationship_counts(dataset)
    
    # Sort by total relationships (descending)
    sorted_tables = sorted(
        counts.items(),
        key=lambda x: x[1]['total'],
        reverse=True
    )
    
    top_tables = {table_name for table_name, _ in sorted_tables[:top_n]}
    
    logger.info(f"Selected top {len(top_tables)} tables")
    return top_tables


def get_table_statistics(dataset: DatasetSchema) -> Dict:
    """
    Get comprehensive statistics about the dataset.
    
    Returns:
        Dictionary with statistics including relationship distribution
    """
    counts = calculate_relationship_counts(dataset)
    centrality = calculate_centrality_scores(dataset)
    
    # Calculate distribution
    relationship_distribution = defaultdict(int)
    for count_data in counts.values():
        total = count_data['total']
        relationship_distribution[total] += 1
    
    # Find tables with most relationships
    sorted_by_relationships = sorted(
        counts.items(),
        key=lambda x: x[1]['total'],
        reverse=True
    )
    
    return {
        'total_tables': len(dataset.tables),
        'tables_with_relationships': len([t for t, c in counts.items() if c['total'] > 0]),
        'tables_without_relationships': len([t for t, c in counts.items() if c['total'] == 0]),
        'relationship_distribution': dict(relationship_distribution),
        'top_tables': [
            {'name': name, 'total': data['total'], 'incoming': data['incoming'], 'outgoing': data['outgoing']}
            for name, data in sorted_by_relationships[:20]
        ],
        'average_relationships': sum(c['total'] for c in counts.values()) / len(counts) if counts else 0
    }

