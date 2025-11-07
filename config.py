"""
Configuration management for BigQuery schema documentation tool.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class Config:
    """Configuration for schema extraction and documentation generation."""
    
    input_path: Path
    input_format: Optional[str] = None  # 'csv', 'json', or None for auto-detect
    output_dir: Path = Path("./schema-docs")
    output_formats: List[str] = None  # ['text', 'uml', 'json', 'yaml', 'csv']
    
    # Simplified diagram options
    simplified: bool = False
    min_relationships: int = 2
    include_tables: Optional[List[str]] = None
    exclude_patterns: Optional[List[str]] = None
    top_n: Optional[int] = None
    
    def __post_init__(self):
        """Normalize paths and set defaults."""
        self.input_path = Path(self.input_path)
        self.output_dir = Path(self.output_dir)
        
        if self.output_formats is None:
            self.output_formats = ['text', 'uml', 'json', 'yaml']
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

