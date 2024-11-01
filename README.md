# SAP HANA Natural Language Query System

## Overview
This system provides a natural language interface for querying SAP HANA databases. It allows users to ask questions in plain English and receives both SQL queries and summarized results. The system uses Azure OpenAI's GPT-4 model to translate natural language questions into SQL queries and summarize the results.

## Key Features
- Natural language to SQL query conversion
- Automatic handling of SAP HANA's DATS date format
- Table relationship management
- Column aliasing and mapping
- Query result summarization
- Interactive table information viewing

## Core Components

### 1. Table Relationship Manager
- Manages database table metadata and relationships
- Key functionalities:
  - Discovers and tracks common columns between tables
  - Handles column aliases for intuitive querying
  - Manages date column identification
  - Provides table relationship information

### 2. Query Generation System
- Utilizes Azure OpenAI's GPT-4 model
- Converts natural language questions to SAP HANA SQL
- Features:
  - Handles relative date references (e.g., "last 7 days")
  - Applies business-friendly column mappings
  - Ensures proper DATS format for dates
  - Validates queries against allowed tables

### 3. Result Processing
- Executes queries against SAP HANA database
- Generates natural language summaries of results
- Provides both raw data and business-friendly explanations

## Key Functions

### `format_date_for_dats(date_str)`
- Converts various date formats to SAP HANA's DATS format (YYYYMMDD)
- Handles multiple input date formats

### `generate_hana_query(question, schema_name, relationship_manager)`
- Processes natural language questions into SQL queries
- Handles relative date references
- Applies column mappings
- Ensures proper date formatting

### `process_query_with_summary(question, schema_name, hana_db, relationship_manager)`
- Complete pipeline for query processing:
  1. Generates SQL query from question
  2. Executes query against database
  3. Summarizes results in natural language

## Configuration Requirements
- Azure OpenAI API credentials
- SAP HANA database connection details
- Predefined allowed tables list
- Column mapping configurations

## Usage Example
```python
# Initialize the system
hana_db = HanaDbConnector()
relationship_manager = TableRelationshipManager(hana_db)

# Process a natural language query
result = process_query_with_summary(
    "Show me sales from last week",
    "your_schema",
    hana_db,
    relationship_manager
)

# Access results
print(result["query"])        # Generated SQL query
print(result["raw_results"])  # Raw query results
print(result["summary"])      # Natural language summary
```

## Security Features
- Restricted to predefined allowed tables
- No DML operations (INSERT, UPDATE, DELETE) allowed
- Schema validation
- Error handling for invalid queries

## Best Practices
1. Always define allowed tables explicitly
2. Use proper column mappings for business terms
3. Maintain clear table relationships
4. Handle date formats consistently
5. Limit query results when appropriate

## Error Handling
- Comprehensive error checking for:
  - Database connections
  - Query generation
  - Query execution
  - Date format conversion
  - Schema validation

## Dependencies
- langchain
- langchain_openai
- python-dotenv
- datetime
- re (regular expressions)
- Azure OpenAI services

## Note
This system is specifically designed for SAP HANA databases and handles their unique requirements, such as the DATS date format and specific column naming conventions.
