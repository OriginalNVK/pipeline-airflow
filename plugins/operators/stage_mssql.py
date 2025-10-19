from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
import pyodbc
import pandas as pd
import json
import os
from typing import Optional


class StageToMSSQLOperator(BaseOperator):
    """
    Custom Operator to stage data from CSV/JSON files to MS SQL Server
    
    :param mssql_conn_id: Connection ID for MS SQL Server
    :param table: Target table name in SQL Server
    :param file_path: Path to data file (CSV or JSON)
    :param file_format: Format of file ('csv' or 'json')
    :param truncate_table: Whether to truncate table before loading
    :param json_path: JSONPath for parsing nested JSON (optional)
    """
    
    ui_color = '#358140'
    
    template_fields = ('file_path',)
    
    @apply_defaults
    def __init__(
        self,
        mssql_conn_id: str = "mssql_default",
        table: str = "",
        file_path: str = "",
        file_format: str = "csv",
        truncate_table: bool = False,
        json_path: Optional[str] = None,
        delimiter: str = ",",
        encoding: str = "utf-8",
        *args, **kwargs
    ):
        super(StageToMSSQLOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.table = table
        self.file_path = file_path
        self.file_format = file_format.lower()
        self.truncate_table = truncate_table
        self.json_path = json_path
        self.delimiter = delimiter
        self.encoding = encoding
    
    def execute(self, context):
        self.log.info(f"Starting to stage data to MS SQL Server table: {self.table}")
        
        # Get connection
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Truncate table if needed
            if self.truncate_table:
                self.log.info(f"Truncating table {self.table}")
                cursor.execute(f"TRUNCATE TABLE {self.table}")
                conn.commit()
            
            # Read data based on format
            if self.file_format == 'csv':
                df = self._read_csv()
            elif self.file_format == 'json':
                df = self._read_json()
            else:
                raise ValueError(f"Unsupported file format: {self.file_format}")
            
            if df.empty:
                self.log.warning("No data to load")
                return
            
            # Insert data
            self._bulk_insert(df, cursor, conn)
            
            self.log.info(f"Successfully staged {len(df)} rows to {self.table}")
            
        except Exception as e:
            self.log.error(f"Error staging data: {str(e)}")
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
    
    def _read_csv(self):
        """Read CSV file"""
        self.log.info(f"Reading CSV file: {self.file_path}")
        return pd.read_csv(
            self.file_path,
            delimiter=self.delimiter,
            encoding=self.encoding
        )
    
    def _read_json(self):
        """Read JSON file"""
        self.log.info(f"Reading JSON file: {self.file_path}")
        
        # Handle JSON Lines format (common for logs)
        if self.file_path.endswith('.jsonl') or 'log_data' in self.file_path:
            return pd.read_json(self.file_path, lines=True)
        
        # Handle regular JSON
        with open(self.file_path, 'r', encoding=self.encoding) as f:
            data = json.load(f)
            
        # Apply JSONPath if provided
        if self.json_path:
            # Simple JSONPath implementation
            keys = self.json_path.strip('$.').split('.')
            for key in keys:
                data = data.get(key, data)
        
        return pd.DataFrame([data] if isinstance(data, dict) else data)
    
    def _bulk_insert(self, df, cursor, conn):
        """Bulk insert data using parameterized queries"""
        self.log.info("Performing bulk insert")
        
        # Prepare column names and placeholders
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['?' for _ in df.columns])
        
        insert_query = f"INSERT INTO {self.table} ({columns}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(row) for row in df.to_numpy()]
        
        # Batch insert
        batch_size = 1000
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            conn.commit()
            self.log.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} rows")