FROM apache/airflow:2.10.4

USER root

# Install ODBC Driver 18 for SQL Server
RUN apt-get update \
    && apt-get install -y gnupg2 curl ca-certificates \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages for MSSQL
RUN pip install --no-cache-dir \
    apache-airflow-providers-microsoft-mssql \
    pyodbc
