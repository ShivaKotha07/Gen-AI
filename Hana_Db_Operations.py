from hdbcli import dbapi
from config import Config

class HanaDbConnector:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.current_schema = None

    def establish_conn(self):
        """
        Establish a connection to the HANA database if not already connected.
        """
        if not self.conn:
            self.conn = dbapi.connect(
                address=Config.hanadb_address,
                port=Config.hanadb_port,
                user=Config.hanadb_user,
                password=Config.hanadb_pass,
            )
            self.cursor = self.conn.cursor()

    def close_conn(self):
        """
        Close the connection to the HANA database.
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.conn = None
        self.cursor = None

    def list_schemas(self):
        """
        Lists all non-system schemas in HANA DB
        """
        try:
            self.establish_conn()
            self.cursor.execute("""
                SELECT SCHEMA_NAME 
                FROM SYS.SCHEMAS 
                WHERE SCHEMA_NAME NOT LIKE '_SYS%'
                ORDER BY SCHEMA_NAME
            """)
            schemas = [row[0] for row in self.cursor.fetchall()]
            return schemas, None
        except Exception as e:
            return None, str(e)

    def select_schema(self, schema_name):
        """
        Sets the current schema
        """
        try:
            self.establish_conn()
            self.cursor.execute(f"SET SCHEMA {schema_name}")
            self.current_schema = schema_name
            return True, None
        except Exception as e:
            return False, str(e)

    def list_tables(self):
        """
        Lists all tables in the current schema
        """
        try:
            if not self.current_schema:
                return None, "No schema selected. Please select a schema first."
            
            self.establish_conn()
            self.cursor.execute(f"""
                SELECT TABLE_NAME 
                FROM TABLES 
                WHERE SCHEMA_NAME = '{self.current_schema}'
                ORDER BY TABLE_NAME
            """)
            tables = [row[0] for row in self.cursor.fetchall()]
            return tables, None
        except Exception as e:
            return None, str(e)

    def list_columns(self, table_name):
        """
        Lists all columns for a specified table
        """
        try:
            if not self.current_schema:
                return None, "No schema selected. Please select a schema first."
            
            self.establish_conn()
            self.cursor.execute(f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE_NAME,
                    LENGTH,
                    IS_NULLABLE
                FROM TABLE_COLUMNS 
                WHERE SCHEMA_NAME = '{self.current_schema}' 
                AND TABLE_NAME = '{table_name}'
                ORDER BY POSITION
            """)
            columns = [
                {
                    'name': row[0],
                    'type': row[1],
                    'length': row[2],
                    'nullable': row[3]
                }
                for row in self.cursor.fetchall()
            ]
            return columns, None
        except Exception as e:
            return None, str(e)

    def execute_query(self, query):
        """
        Executes a query and returns the results
        """
        try:
            self.establish_conn()
            self.cursor.execute(query)
            
            # Check if the query returns results
            if self.cursor.description:
                columns = [desc[0] for desc in self.cursor.description]
                rows = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
                return rows, None
            else:
                # For queries that don't return results (INSERT, UPDATE, etc.)
                affected_rows = self.cursor.rowcount
                return {'affected_rows': affected_rows}, None
        except Exception as e:
            return None, str(e)

if __name__ == "__main__":
    hana_connector = HanaDbConnector()
    success, error = hana_connector.select_schema('your schema')
    if error:
        print(f"Error selecting schema: {error}")
    else:
        print("Successfully selected schema")
        columns, error = hana_connector.list_columns('your table name')
        if error:
            print(f"Error retrieving columns: {error}")
        else:
            print("\nColumns in table:")
            for column in columns:
                print(f"- {column['name']} ({column['type']}, length: {column['length']}, nullable: {column['nullable']})")

        query = "SELECT TOP 5 * FROM your table"
        results, error = hana_connector.execute_query(query)
        if error:
            print(f"Error executing query: {error}")
        else:
            print("\nSample data from your table:")
            for row in results:
                print(row)

    hana_connector.close_conn()
