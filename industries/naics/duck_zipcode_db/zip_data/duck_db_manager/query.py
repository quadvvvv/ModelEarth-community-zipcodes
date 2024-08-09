import os
import duckdb
import csv
import pandas as pd

class DataQueryManager:
    def __init__(self, db_path, export_dir):
        """
        Initializes the DataQueryManager with the given database path and export directory.

        Args:
            db_path (str): The path to the DuckDB database file.
            export_dir (str): The directory where exported data will be saved.
        """
        self.db_path = db_path  # Path to the DuckDB database
        self.export_dir = export_dir  # Directory for exporting data

    def _connect_db(self):
        """
        Establishes a connection to the DuckDB database.

        Returns:
            duckdb.DuckDBPyConnection: The database connection object.
        """
        return duckdb.connect(self.db_path)  # Connect to the database using the specified path

    def execute_query(self, query):
        """
        Executes a SQL query on the database and returns the results as a DataFrame.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            pd.DataFrame: The result of the query as a DataFrame.
        """
        with self._connect_db() as conn:  # Establish a connection to the database
            result = conn.execute(query)  # Execute the query
            return result.fetchdf()  # Fetch the result as a DataFrame

    def filter(self, zipcode, year=None, industry_level=None, conn=None):
        """
        Filters the data based on the provided criteria.

        Args:
            zipcode (str): The 5-digit zip code to filter by.
            year (int, optional): The year to filter by.
            industry_level (str, optional): The industry level to filter by.
            conn (duckdb.DuckDBPyConnection, optional): An existing database connection.

        Returns:
            pd.DataFrame: The filtered results as a DataFrame.

        Raises:
            ValueError: If the provided zip code is not exactly 5 digits long.
        """
        # Validate the zip code
        if not zipcode or not zipcode.isdigit() or len(zipcode) != 5:
            raise ValueError("Zip code must be exactly 5 digits long and is required.")
        
        # Build the query conditions
        conditions = [f"SUBSTR(GeoID, 1, 5) = '{zipcode}'"]  # Condition for the zip code
        if year:
            conditions.append(f"Year = {year}")  # Add condition for the year if provided
        if industry_level:
            conditions.append(f"IndustryLevel = {industry_level}")  # Add condition for industry level if provided

        # Construct the SQL query
        query = "SELECT GeoID AS Zipcode, NaicsCode, Establishments, Employees, Payroll FROM DataEntry WHERE " + " AND ".join(conditions)
        
        # Execute the query either with an existing connection or a new one
        if conn is None:
            with duckdb.connect(self.db_path) as conn:  # Connect if no connection is provided
                return conn.execute(query).fetchdf()  # Fetch results as a DataFrame
        else:
            return conn.execute(query).fetchdf()  # Fetch results using the provided connection
