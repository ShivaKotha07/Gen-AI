from datetime import datetime, timedelta
import re
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain_openai import AzureChatOpenAI
from dotenv import load_dotenv
from config import Config
from Hana_Db_Operations import HanaDbConnector

load_dotenv()

ALLOWED_TABLES = ['your tables list']

COLUMN_MAPPINGS = {
    'if you have any business columns use this for mapping'
    'below is the example usage'
    'Product Category':'prdcat'
}

class TableRelationshipManager:
    def __init__(self, hana_db):
        self.hana_db = hana_db
        self.table_columns = {}
        self.common_columns = {}
        self.column_aliases = {}
        self.date_columns = {}
        self._initialize_table_info()

    def _initialize_table_info(self):
        """Initialize table column information and find common columns"""
        print("\nInitializing table information...")
        
        # Clear existing data
        self.table_columns.clear()
        self.common_columns.clear()
        
        for table in ALLOWED_TABLES:
            print(f"Fetching columns for table: {table}")
            columns, error = self.hana_db.list_columns(table)
            if error:
                print(f"Error fetching columns for {table}: {error}")
                continue
                
            if columns:
                print(f"Found {len(columns)} columns for {table}")
                self.table_columns[table] = columns
                self._identify_date_columns(table, columns)
                self._create_column_aliases(table, columns)
            else:
                print(f"No columns found for {table}")

        # Find relationships between tables
        print("\nFinding table relationships...")
        for i, table1 in enumerate(ALLOWED_TABLES):
            for table2 in ALLOWED_TABLES[i+1:]:
                if table1 in self.table_columns and table2 in self.table_columns:
                    common = self._find_common_columns(table1, table2)
                    if common:
                        key = f"{table1}_{table2}"
                        self.common_columns[key] = common
                        print(f"Found relationship between {table1} and {table2}")

    def _identify_date_columns(self, table, columns):
        """Identify columns that use DATS format"""
        date_cols = {}
        for col in columns:
            if col['type'].upper() in ['DATS', 'D', 'DATE']:
                date_cols[col['name']] = 'DATS'
        self.date_columns[table] = date_cols

    def get_date_columns(self, table=None):
        """Get all date columns, optionally filtered by table"""
        if table:
            return self.date_columns.get(table, {})
        return self.date_columns

    def _create_column_aliases(self, table, columns):
        """Create aliases for columns to handle common variations"""
        aliases = {}
        for col in columns:
            col_name = col['name']
            aliases[col_name.lower()] = col_name
            # Add common variations
            if col_name in ['DESCN']:
                aliases['description'] = col_name
                aliases['desc'] = col_name
            elif col_name in ['BPROC']:
                aliases['process'] = col_name
            elif col_name in ['LANGU']:
                aliases['language'] = col_name
            elif col_name in ['REQTYPE']:
                aliases['request type'] = col_name
                aliases['request types'] = col_name
            elif col_name in ['REQ_CREATED']:
                aliases['request created date'] = col_name
                aliases['request creation date'] = col_name
        self.column_aliases[table] = aliases

    def get_actual_column_name(self, table, column_alias):
        """Get the actual column name from an alias"""
        if table in self.column_aliases:
            return self.column_aliases[table].get(column_alias.lower())
        return None

    def _find_common_columns(self, table1, table2):
        """Find common columns between two tables"""
        if table1 not in self.table_columns or table2 not in self.table_columns:
            return []
        
        cols1 = {col['name'] for col in self.table_columns[table1]}
        cols2 = {col['name'] for col in self.table_columns[table2]}
        return list(cols1.intersection(cols2))

    def get_table_relationships(self):
        """Get a formatted string of table relationships"""
        relationships = []
        seen_pairs = set()
        
        for key, columns in self.common_columns.items():
            table1, table2 = key.split('_')
            if (table1, table2) not in seen_pairs:
                relationships.append(f"Tables {table1} and {table2} share columns: {', '.join(columns)}")
                seen_pairs.add((table1, table2))
        
        return "\n".join(relationships) if relationships else "No relationships found between tables."

    def get_all_columns_info(self):
        """Get formatted information about all columns in all tables"""
        info = []
        for table in ALLOWED_TABLES:
            if table in self.table_columns:
                cols = [f"{col['name']} ({col['type']}) - Aliases: {self._get_column_aliases(table, col['name'])}" 
                       for col in self.table_columns[table]]
                info.append(f"{table} columns:\n  " + "\n  ".join(cols))
        return "\n".join(info)

    def _get_column_aliases(self, table, column_name):
        """Get all aliases for a column"""
        aliases = []
        for alias, actual in self.column_aliases[table].items():
            if actual == column_name and alias != column_name.lower():
                aliases.append(alias)
        return ", ".join(aliases) if aliases else "no aliases"

    def get_table_info(self, table_name):
        """Get column information for a specific table"""
        if table_name in self.table_columns:
            return ", ".join([f"{col['name']} ({col['type']})" for col in self.table_columns[table_name]])
        return None
llm = AzureChatOpenAI(
    deployment_name="gpt-4",
    model_name="gpt-4",
    temperature=0,
    openai_api_key=Config.OPENAI_API_KEY,
    openai_api_version="2023-03-15-preview",
    azure_endpoint=Config.OPENAI_API_KEY_ENDPOINT,
)
def format_date_for_dats(date_str):
    """Convert a date string to SAP DATS format (YYYYMMDD)"""
    try:
        if isinstance(date_str, datetime):
            return date_str.strftime('%Y%m%d')
        if isinstance(date_str, str):
            # Handle various date formats
            date_formats = [
                '%Y-%m-%d',
                '%d-%m-%Y',
                '%Y/%m/%d',
                '%d/%m/%Y',
                '%Y%m%d'
            ]
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_str, fmt).strftime('%Y%m%d')
                except ValueError:
                    continue
        raise ValueError(f"Unable to parse date: {date_str}")
    except Exception as e:
        raise ValueError(f"Date conversion error: {str(e)}")

def get_relative_date(period):
    """Get date for relative time periods"""
    today = datetime.now()
    if 'year' in period:
        return today - timedelta(days=365)
    elif 'month' in period:
        return today - timedelta(days=30)
    elif 'week' in period:
        return today - timedelta(days=7)
    elif 'day' in period:
        days = int(re.search(r'\d+', period).group()) if re.search(r'\d+', period) else 1
        return today - timedelta(days=days)
    return today

def process_date_conditions(query, relationship_manager):
    """Process and format all date conditions in the query"""
    # Get all date columns across all tables
    all_date_columns = []
    for table in ALLOWED_TABLES:
        date_cols = relationship_manager.get_date_columns(table)
        all_date_columns.extend(date_cols.keys())

    # Regular expressions for finding date conditions
    date_patterns = [
        r"(\w+)\s*([><=]+)\s*'(\d{4}-\d{2}-\d{2})'",  # ISO format
        r"(\w+)\s*([><=]+)\s*'(\d{2}/\d{2}/\d{4})'",  # DD/MM/YYYY
        r"(\w+)\s*([><=]+)\s*'(\d{2}-\d{2}-\d{4})'",  # DD-MM-YYYY
    ]

    processed_query = query
    for pattern in date_patterns:
        matches = re.finditer(pattern, processed_query)
        for match in matches:
            column, operator, date_str = match.groups()
            if column in all_date_columns:
                try:
                    dats_date = format_date_for_dats(date_str)
                    old_condition = match.group(0)
                    new_condition = f"{column} {operator} '{dats_date}'"
                    processed_query = processed_query.replace(old_condition, new_condition)
                except ValueError:
                    continue

    return processed_query
query_prompt_template = """
You are an AI assistant designed to generate SAP HANA SQL queries based on user questions.
Context:
- Database: SAP HANA
- Schema: {schema_name}
- Available Tables and Their Columns:
{table_columns}

Table Relationships:
{table_relationships}

IMPORTANT NOTES:
1. Use EXACT column names as shown above (e.g., use 'DESCN' for description, 'BPROC' for business process)
2. All date columns use DATS format (YYYYMMDD string format)
   - Format all dates as YYYYMMDD (e.g., '20231024' for October 24, 2023)
   - Use string comparison operators for dates (e.g., COLUMN_NAME >= '20231024')
   - Do not use DATE or other date functions on DATS columns
3. Common column mappings:
   - For description, use 'DESCN'
   - For business process, use 'BPROC'
   - For language, use 'LANGU'
   - For request type, use 'REQTYPE'

User Question: {question}

Guidelines:
1. Create a syntactically correct SAP HANA SQL query using proper DATS format for ALL date columns to answer the question.
2. Unless specified otherwise, limit results to 10 rows.
3. Only query for relevant columns, not all columns.
4. Use appropriate JOIN conditions based on common columns between the tables.
5. Do not use any DML statements (INSERT, UPDATE, DELETE, DROP, etc.).
6. Always use YYYYMMDD string format for date comparisons
7. Consider using ORDER BY for meaningful results
8. Only use tables from: {allowed_tables}

Generate the SAP HANA SQL query:
"""

summarization_prompt_template = """
Given the following query results from an SAP HANA database, provide a clear and concise summary:

Original Question: {question}
Query Used: {query}
Results: {results}

Please provide a summary that:
1. Answers the original question directly
2. Highlights key findings
3. Mentions any notable patterns or insights
4. Uses business-friendly language
5. If multiple tables were joined, explain the relationships used

Summary:
"""

query_prompt = PromptTemplate(
    input_variables=["schema_name", "table_columns", "table_relationships", "question", "allowed_tables"],
    template=query_prompt_template
)

summarization_prompt = PromptTemplate(
    input_variables=["question", "query", "results"],
    template=summarization_prompt_template
)

query_chain = LLMChain(llm=llm, prompt=query_prompt)
summary_chain = LLMChain(llm=llm, prompt=summarization_prompt)

def generate_hana_query(question, schema_name, relationship_manager):
    """Generate a HANA SQL query based on the question and available table information"""
    processed_question = question

    # Handle relative date references
    date_patterns = [
        (r'last (\d+) days', 'days'),
        (r'past (\d+) days', 'days'),
        (r'last year', 'year'),
        (r'past year', 'year'),
        (r'last month', 'month'),
        (r'past month', 'month'),
        (r'last week', 'week'),
        (r'past week', 'week')
    ]

    for pattern, period in date_patterns:
        if re.search(pattern, processed_question, re.IGNORECASE):
            relative_date = get_relative_date(period)
            processed_question = re.sub(
                pattern,
                f"since {format_date_for_dats(relative_date)}",
                processed_question,
                flags=re.IGNORECASE
            )

    # Handle column mappings
    for common_name, actual_name in COLUMN_MAPPINGS.items():
        processed_question = processed_question.replace(common_name, actual_name)

    # Generate initial query
    result = query_chain.run({
        "schema_name": schema_name,
        "table_columns": relationship_manager.get_all_columns_info(),
        "table_relationships": relationship_manager.get_table_relationships(),
        "question": processed_question,
        "allowed_tables": ", ".join(ALLOWED_TABLES)
    })

    # Post-process the query to ensure proper date formatting
    final_query = process_date_conditions(result.strip(), relationship_manager)
    
    return final_query

def execute_hana_query(query, hana_db):
    """
    Execute the generated query
    """
    results, error = hana_db.execute_query(query)
    if error:
        raise ValueError(f"Error executing query: {error}")
    return results

def summarize_results(question, query, results):
    """
    Generate a natural language summary of the query results
    """
    summary = summary_chain.run({
        "question": question,
        "query": query,
        "results": str(results)
    })
    return summary.strip()

def process_query_with_summary(question, schema_name, hana_db, relationship_manager):
    """
    Complete process to generate query, execute it, and summarize results
    """
    try:
        generated_query = generate_hana_query(question, schema_name, relationship_manager)
        results = execute_hana_query(generated_query, hana_db)
        summary = summarize_results(question, generated_query, results)
        
        return {
            "query": generated_query,
            "raw_results": results,
            "summary": summary
        }
    except Exception as e:
        return {
            "error": str(e)
        }

def print_system_info(schema_name, relationship_manager):
    """Print system information with verification"""
    print(f"\nUsing schema: {schema_name}")
    
    # Verify and print available tables
    actual_tables = sorted(list(set(ALLOWED_TABLES)))  # Remove any duplicates
    print(f"Available tables ({len(actual_tables)}):", ", ".join(actual_tables))
    
    # Print table columns for verification
    print("\nVerifying table columns:")
    for table in actual_tables:
        column_count = len(relationship_manager.table_columns.get(table, []))
        print(f"{table}: {column_count} columns")
    
    # Print relationships
    print("\nTable relationships:")
    print(relationship_manager.get_table_relationships())

def print_result(result):
    """Helper function to print results in a formatted way"""
    if "error" in result:
        print("\nError:", result["error"])
        return
    
    print("\nGenerated SAP HANA SQL Query:")
    print(result["query"])
    
    print("\nRaw Query Results:")
    print(result["raw_results"])
    
    print("\nSummary:")
    print(result["summary"])
    print("\n" + "="*80 + "\n")

def main():
    
    # Initialize database connection and relationship manager
    hana_db = HanaDbConnector()
    schema_name = "your schema"
    success, error = hana_db.select_schema(schema_name)
    
    if error:
        print(f"Error selecting schema: {error}")
        return
        
    relationship_manager = TableRelationshipManager(hana_db)
    print_system_info(schema_name, relationship_manager)
    
    while True:
        question = input("\nEnter your question (or 'exit' to quit): ").strip()
        
        if question.lower() in ['exit', 'quit', '']:
            print("Thank you for using the Enhanced SAP HANA Query System. Goodbye!")
            break
            
        try:
            result = process_query_with_summary(question, schema_name, hana_db, relationship_manager)
            print_result(result)
            
            view_details = input("\nWould you like to see detailed information about any table? (table name/n): ").strip()
            if view_details.upper() in ALLOWED_TABLES:
                table_info = relationship_manager.get_table_info(view_details.upper())
                print(f"\nDetailed columns for {view_details.upper()}:")
                print(table_info)
                
        except Exception as e:
            print(f"\nAn error occurred: {str(e)}")
            print("Please try again with a different question.")

if __name__ == "__main__":
    main()