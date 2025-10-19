"""
Load Fact table in MS SQL Server
"""
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
import pyodbc


class LoadFactOperator(BaseOperator):
    """
    Load data into Fact table using SQL transformation
    
    :param mssql_conn_id: Connection ID for MS SQL Server
    :param table: Target fact table name
    :param sql_query: SQL query to transform and load data
    :param append_only: If True, only INSERT. If False, TRUNCATE then INSERT
    """
    
    ui_color = '#F98866'
    
    template_fields = ('sql_query',)
    
    @apply_defaults
    def __init__(
        self,
        mssql_conn_id: str = "",
        table: str = "",
        sql_query: str = "",
        append_only: bool = True,  # Fact tables usually append only
        *args, **kwargs
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_only = append_only
    
    def execute(self, context):
        self.log.info(f"Loading Fact table: {self.table}")
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Truncate if not append only
            if not self.append_only:
                self.log.info(f"Truncating table {self.table}")
                cursor.execute(f"TRUNCATE TABLE {self.table}")
                conn.commit()
            
            # Execute transformation query
            self.log.info("Executing SQL transformation")
            cursor.execute(self.sql_query)
            conn.commit()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {self.table}")
            count = cursor.fetchone()[0]
            
            self.log.info(f"Successfully loaded {count} rows into {self.table}")
            
        except Exception as e:
            self.log.error(f"Error loading fact table: {str(e)}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    
    def _get_connection(self):
        """Get MS SQL Server connection"""
        connection = BaseHook.get_connection(self.mssql_conn_id)
        
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={connection.host};"
            f"DATABASE={connection.schema};"
            f"UID={connection.login};"
            f"PWD={connection.password}"
        )
        
        return pyodbc.connect(conn_str)