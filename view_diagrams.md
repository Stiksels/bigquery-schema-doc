# How to Visualize the Schema Diagrams

## Mermaid Diagram (`schema_diagram.mmd`)

### Option 1: Online Viewer (Easiest)
1. Go to https://mermaid.live/
2. Open `schema-docs/schema_diagram.mmd`
3. Copy the entire contents
4. Paste into the editor
5. The diagram will render automatically

**Note:** For very large diagrams (95 tables), the online viewer may be slow. Consider using Option 2 or 3.

### Option 2: VS Code Extension
1. Install the "Markdown Preview Mermaid Support" extension in VS Code
2. Open `schema_diagram.mmd` in VS Code
3. Use Command Palette (Cmd+Shift+P) → "Markdown: Open Preview"
4. The diagram will render in the preview pane

### Option 3: GitHub/GitLab
- Push the `.mmd` file to a GitHub/GitLab repository
- View it in a Markdown file - it will render automatically

### Option 4: Install Mermaid CLI (for PNG/SVG export)
```bash
# Install Node.js if not already installed
# Then install Mermaid CLI:
npm install -g @mermaid-js/mermaid-cli

# Generate PNG image
mmdc -i schema-docs/schema_diagram.mmd -o schema-docs/schema_diagram.png

# Generate SVG image
mmdc -i schema-docs/schema_diagram.mmd -o schema-docs/schema_diagram.svg
```

## PlantUML Diagram (`schema_diagram.puml`)

### Option 1: Online Viewer
1. Go to http://www.plantuml.com/plantuml/uml/
2. Open `schema-docs/schema_diagram.puml`
3. Copy the entire contents
4. Paste into the editor
5. The diagram will render as an image
6. You can download as PNG, SVG, or other formats

### Option 2: VS Code Extension
1. Install the "PlantUML" extension in VS Code
   - Note: Requires Java to be installed
2. Open `schema_diagram.puml` in VS Code
3. Use Command Palette (Cmd+Shift+P) → "PlantUML: Preview Current Diagram"
4. Or right-click → "Preview PlantUML Diagram"

### Option 3: Install PlantUML (for local rendering)
```bash
# Install Java if not already installed
# Then install PlantUML:
# macOS:
brew install plantuml

# Or download from: http://plantuml.com/download

# Generate PNG image
plantuml -tpng schema-docs/schema_diagram.puml

# Generate SVG image
plantuml -tsvg schema-docs/schema_diagram.puml
```

## Recommendation

For a quick view:
- **Mermaid**: Use https://mermaid.live/ (easiest, no installation)
- **PlantUML**: Use http://www.plantuml.com/plantuml/uml/ (easiest, no installation)

For ongoing use:
- Install VS Code extensions for both formats
- This allows previewing and editing directly in your editor

## Note on Large Diagrams

With 95 tables, the diagrams are quite large. Consider:
- Using the online viewers (they handle large diagrams well)
- Filtering to specific tables if needed
- Using the structured formats (JSON/YAML) for programmatic analysis


