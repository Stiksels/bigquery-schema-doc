"""
Command-line interface for BigQuery schema documentation tool.
"""
import click
from pathlib import Path
from typing import List, Optional

try:
    from .config import Config
except ImportError:
    from config import Config


@click.command()
@click.option(
    '--input', '-i',
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help='Path to input file or directory containing exported schema files'
)
@click.option(
    '--input-format', '-f',
    type=click.Choice(['csv', 'json'], case_sensitive=False),
    default=None,
    help='Input file format (csv or json). Auto-detected if not specified.'
)
@click.option(
    '--output-dir', '-o',
    type=click.Path(path_type=Path),
    default=Path('./schema-docs'),
    help='Output directory for generated documentation (default: ./schema-docs)'
)
@click.option(
    '--formats',
    multiple=True,
    type=click.Choice(['text', 'uml', 'json', 'yaml', 'csv'], case_sensitive=False),
    default=['text', 'uml', 'json', 'yaml'],
    help='Output formats to generate (can specify multiple times). Default: text, uml, json, yaml'
)
@click.option(
    '--simplified',
    is_flag=True,
    default=False,
    help='Generate simplified diagram focusing on core entities with many relationships'
)
@click.option(
    '--min-relationships',
    type=int,
    default=2,
    help='Minimum number of relationships required for simplified diagram (default: 2)'
)
@click.option(
    '--include-tables',
    multiple=True,
    help='Explicitly include specific tables in simplified diagram (can specify multiple times)'
)
@click.option(
    '--exclude-patterns',
    multiple=True,
    help='Patterns to exclude from simplified diagram (e.g., "*_view_bi", "*_dump") (can specify multiple times)'
)
@click.option(
    '--top-n',
    type=int,
    default=None,
    help='Include top N tables by relationship count in simplified diagram'
)
def main(
    input: Path,
    input_format: Optional[str],
    output_dir: Path,
    formats: tuple,
    simplified: bool,
    min_relationships: int,
    include_tables: tuple,
    exclude_patterns: tuple,
    top_n: Optional[int]
) -> None:
    """
    Generate BigQuery schema documentation from manually exported files.
    
    This tool processes CSV or JSON exports from BigQuery and generates
    comprehensive documentation in multiple formats suitable for semantic
    mapping workshops.
    """
    # Convert formats tuple to list
    formats_list = list(formats) if formats else ['text', 'uml', 'json', 'yaml']
    
    # Create config
    config = Config(
        input_path=input,
        input_format=input_format,
        output_dir=output_dir,
        output_formats=formats_list
    )
    
    # Add simplified diagram options to config
    config.simplified = simplified
    config.min_relationships = min_relationships
    config.include_tables = list(include_tables) if include_tables else None
    config.exclude_patterns = list(exclude_patterns) if exclude_patterns else None
    config.top_n = top_n
    
    # Import here to avoid circular imports
    try:
        from .main import process_schema_files
    except ImportError:
        from main import process_schema_files
    
    try:
        process_schema_files(config)
        click.echo(f"✓ Documentation generated successfully in {output_dir}")
    except Exception as e:
        click.echo(f"✗ Error: {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    main()

