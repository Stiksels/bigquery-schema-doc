"""
Main entry point for BigQuery schema documentation tool.
"""
import logging
from pathlib import Path
from typing import List

try:
    from .config import Config
    from .extract_schema import build_schema_model
    from .generators.text_generator import generate_text_documentation
    from .generators.uml_generator import generate_plantuml_diagram, generate_mermaid_diagram
    from .generators.structured_generator import (
        generate_json_schema,
        generate_yaml_schema,
        generate_csv_export
    )
    from .schema_filter import create_simplified_schema, FilterConfig
except ImportError:
    from config import Config
    from extract_schema import build_schema_model
    from generators.text_generator import generate_text_documentation
    from generators.uml_generator import generate_plantuml_diagram, generate_mermaid_diagram
    from generators.structured_generator import (
        generate_json_schema,
        generate_yaml_schema,
        generate_csv_export
    )
    from schema_filter import create_simplified_schema, FilterConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_schema_files(config: Config) -> None:
    """
    Process exported schema files and generate documentation.
    
    Args:
        config: Configuration object with input/output paths and formats
    """
    logger.info("Starting schema documentation generation")
    logger.info(f"Input: {config.input_path}")
    logger.info(f"Output directory: {config.output_dir}")
    logger.info(f"Output formats: {', '.join(config.output_formats)}")
    
    # Collect input files
    input_files = _collect_input_files(config.input_path)
    
    if not input_files:
        raise ValueError(f"No input files found at: {config.input_path}")
    
    logger.info(f"Found {len(input_files)} input file(s)")
    
    # Build schema model
    logger.info("Building schema model from input files...")
    dataset = build_schema_model(input_files, config.input_format)
    
    stats = dataset.get_statistics()
    logger.info(f"Schema model created: {stats['table_count']} tables, "
                f"{stats['column_count']} columns, {stats['relationship_count']} relationships")
    
    # Create simplified schema if requested
    simplified_dataset = None
    if config.simplified:
        logger.info("Creating simplified schema...")
        
        # Load product-specific always-included tables from config
        from extract_schema import load_included_tables_from_config
        product_include_tables = []
        if input_files:
            # Use first input file to find config
            product_include_tables = load_included_tables_from_config(input_files[0])
        
        # Combine CLI include_tables with product-specific ones
        all_include_tables = list(config.include_tables) if config.include_tables else []
        all_include_tables.extend(product_include_tables)
        
        filter_config = FilterConfig(
            min_relationships=config.min_relationships,
            include_tables=all_include_tables if all_include_tables else None,
            exclude_patterns=config.exclude_patterns,
            top_n=config.top_n
        )
        simplified_dataset = create_simplified_schema(dataset, filter_config)
        simplified_stats = simplified_dataset.get_statistics()
        logger.info(f"Simplified schema: {simplified_stats['table_count']} tables "
                   f"(reduced from {stats['table_count']}), "
                   f"{simplified_stats['relationship_count']} relationships "
                   f"(from {stats['relationship_count']})")
    
    # Generate outputs for full schema
    if 'text' in config.output_formats:
        output_path = config.output_dir / 'schema_documentation.md'
        generate_text_documentation(dataset, output_path)
    
    if 'uml' in config.output_formats:
        puml_path = config.output_dir / 'schema_diagram.puml'
        generate_plantuml_diagram(dataset, puml_path)
        
        mermaid_path = config.output_dir / 'schema_diagram.mmd'
        generate_mermaid_diagram(dataset, mermaid_path)
    
    if 'json' in config.output_formats:
        output_path = config.output_dir / 'schema.json'
        generate_json_schema(dataset, output_path)
    
    if 'yaml' in config.output_formats:
        output_path = config.output_dir / 'schema.yaml'
        generate_yaml_schema(dataset, output_path)
    
    if 'csv' in config.output_formats:
        output_path = config.output_dir / 'schema_export.csv'
        generate_csv_export(dataset, output_path)
    
    # Generate simplified outputs if requested
    if config.simplified and simplified_dataset:
        logger.info("Generating simplified documentation...")
        
        if 'text' in config.output_formats:
            output_path = config.output_dir / 'schema_documentation_simplified.md'
            generate_text_documentation(simplified_dataset, output_path)
        
        if 'uml' in config.output_formats:
            puml_path = config.output_dir / 'schema_diagram_simplified.puml'
            generate_plantuml_diagram(simplified_dataset, puml_path)
            
            mermaid_path = config.output_dir / 'schema_diagram_simplified.mmd'
            generate_mermaid_diagram(simplified_dataset, mermaid_path)
        
        if 'json' in config.output_formats:
            output_path = config.output_dir / 'schema_simplified.json'
            generate_json_schema(simplified_dataset, output_path)
        
        if 'yaml' in config.output_formats:
            output_path = config.output_dir / 'schema_simplified.yaml'
            generate_yaml_schema(simplified_dataset, output_path)
        
        if 'csv' in config.output_formats:
            output_path = config.output_dir / 'schema_export_simplified.csv'
            generate_csv_export(simplified_dataset, output_path)
    
    logger.info("Documentation generation complete")


def _collect_input_files(input_path: Path) -> List[Path]:
    """
    Collect input files from path (file or directory).
    
    Args:
        input_path: Path to file or directory
    
    Returns:
        List of file paths to process
    """
    input_files = []
    
    if input_path.is_file():
        # Single file
        input_files.append(input_path)
    elif input_path.is_dir():
        # Directory - collect all CSV and JSON files
        for ext in ['*.csv', '*.json']:
            input_files.extend(input_path.glob(ext))
            input_files.extend(input_path.glob(ext.upper()))
    else:
        raise ValueError(f"Input path does not exist: {input_path}")
    
    return sorted(input_files)


if __name__ == '__main__':
    # If run directly, use CLI
    try:
        from .cli import main as cli_main
    except ImportError:
        from cli import main as cli_main
    cli_main()

