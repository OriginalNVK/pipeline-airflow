"""
Script to setup Airflow connection for MS SQL Server
Run this after starting Airflow
"""
import subprocess
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def create_mssql_connection():
    """Create MS SQL Server connection in Airflow"""
    
    # Get config from environment
    server = os.getenv('MSSQL_SERVER', 'localhost')
    port = os.getenv('MSSQL_PORT', '1433')
    database = os.getenv('MSSQL_DATABASE', 'sparkify')
    username = os.getenv('MSSQL_USERNAME', 'sa')
    password = os.getenv('MSSQL_PASSWORD', '')
    
    # Build connection URI
    conn_id = 'mssql_default'
    conn_type = 'mssql'
    
    # Airflow CLI command to add connection
    cmd = [
        'airflow', 'connections', 'add', conn_id,
        '--conn-type', conn_type,
        '--conn-host', server,
        '--conn-port', str(port),
        '--conn-schema', database,
        '--conn-login', username,
        '--conn-password', password
    ]
    
    try:
        # Delete existing connection if exists
        subprocess.run(
            ['airflow', 'connections', 'delete', conn_id],
            check=False,
            capture_output=True
        )
        
        # Add new connection
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"✓ Successfully created Airflow connection: {conn_id}")
        print(f"  Server: {server}:{port}")
        print(f"  Database: {database}")
        print(f"  Username: {username}")
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Error creating connection: {e}")
        print(f"Output: {e.output}")
        raise


def verify_connection():
    """Verify the connection was created"""
    try:
        result = subprocess.run(
            ['airflow', 'connections', 'get', 'mssql_default'],
            check=True,
            capture_output=True,
            text=True
        )
        print("\n✓ Connection verified successfully!")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"✗ Error verifying connection: {e}")


def main():
    print("="*50)
    print("Airflow MS SQL Server Connection Setup")
    print("="*50 + "\n")
    
    create_mssql_connection()
    verify_connection()
    
    print("\n" + "="*50)
    print("Setup completed!")
    print("="*50)


if __name__ == '__main__':
    main()