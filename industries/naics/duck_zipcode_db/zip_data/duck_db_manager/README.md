# Data Management and Querying with DuckDB

This repository contains Python scripts and a Jupyter notebook for managing and querying data using DuckDB. The project includes modules for database management, data exporting, and executing queries. **Note: This part of the project does not cover how we pull APIs to populate the database or create new annual database. The `DuckDBManager` class logic is based on existing databases or recovering databases from `exported_tables`, which serve as backups in CSV files.**

## Files

### 1. `duckdb_manager.py`
This module defines the `DuckDBManager` class, which is responsible for managing the connection to a DuckDB database and performing various database operations if needed.

### 2. `dataexporter.py` (deprecated)
This module defines the `DataExporter` class, which handles the exporting of data from the DuckDB database to nested zip folders.

### 3. `query.py`
This file defines the `DataQueryManager` class, which is responsible for executing SQL queries and filtering data based on specified criteria, instead of using a database instance.

### 4. `duckdb_database.ipynb`
A Jupyter notebook that serves as an interactive interface for exploring the DuckDB database. It includes example queries, data visualizations, and demonstrations of how to use the `DataQueryManager` class to retrieve and analyze data.

## Requirements

TBD
