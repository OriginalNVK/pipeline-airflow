#!/bin/bash
# Wait for SQL Server to start
sleep 30s

# Create airflow database
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "Airflow@2024" -C -Q "CREATE DATABASE airflow"

echo "Database airflow created successfully"
