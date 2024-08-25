import duckdb
import os
import pandas as pd
from tqdm import tqdm
import os
import math

class DuckDBManager:
    """
    A class to manage DuckDB database operations for US economic data.

    Attributes:
        db_path (str): Path to the DuckDB database file.
        export_dir (str): Directory where CSV files are exported.
    """

    def __init__(self, db_path='database/us_economic_data.duckdb', export_dir='database/exported_tables'):
        """
        Initializes the DuckDBManager with specified database path and export directory.

        Args:
            db_path (str): Path to the DuckDB database file. Default is 'database/us_economic_data.duckdb'.
            export_dir (str): Directory for exporting CSV files. Default is 'database/exported_tables'.
        """
        self.db_path = db_path
        self.export_dir = export_dir
        self._connect_db()  # Establishes connection to the database.
        self._close_db()    # Closes the connection after initialization.

    def _connect_db(self):
        """
        Connects to the DuckDB database. If the database does not exist, it creates a new one,
        initializes tables, imports CSV files, and creates indexes.
        """
        try:
            if os.path.exists(self.db_path):
                self.conn = duckdb.connect(self.db_path)  # Connect to existing database.
            else:
                print(f"Database {self.db_path} does not exist. Creating new database...")
                self.conn = duckdb.connect(self.db_path)  # Create new database.
                self._create_tables()                      # Create necessary tables.
                self.import_all_csv_files()                # Import CSV files into tables.
                self.create_indexes()                      # Create indexes for optimization.
        except Exception as e:
            print(f"Error connecting to database or initializing: {e}")

    def _close_db(self):
        """
        Closes the connection to the DuckDB database.
        """
        try:
            if self.conn:
                self.conn.close()  # Close the connection if it exists.
                self.conn = None
        except Exception as e:
            print(f"Error closing database connection: {e}")
            
    def create_indexes(self):
        """
        Creates indexes on various tables in the database to optimize query performance.
        """
        self._connect_db()  # Ensure database connection is established.
        print("Creating indexes...")
        index_queries = [
            ('CREATE INDEX IF NOT EXISTS idx_GeoID ON DimZipCode(GeoID)', 'Index on GeoID in DimZipCode'),
            ('CREATE INDEX IF NOT EXISTS idx_NaicsCode ON DimNaics(NaicsCode)', 'Index on NaicsCode in DimNaics'),
            ('CREATE INDEX IF NOT EXISTS idx_Year ON DimYear(Year)', 'Index on Year in DimYear'),
            ('CREATE INDEX IF NOT EXISTS idx_EntryID ON DataEntry(EntryID)', 'Index on EntryID in DataEntry'),
            ('CREATE INDEX IF NOT EXISTS idx_GeoID_DataEntry ON DataEntry(GeoID)', 'Index on GeoID in DataEntry'),
            ('CREATE INDEX IF NOT EXISTS idx_NaicsCode_DataEntry ON DataEntry(NaicsCode)', 'Index on NaicsCode in DataEntry')
        ]

        try:
            for query, description in tqdm(index_queries, desc="Creating Indexes"):
                tqdm.write(f"Creating Index for {description}...")
                self.conn.execute(query)  # Execute index creation query.
                tqdm.write(f"{description} created successfully.")
        except Exception as e:
            print(f"Error creating indexes: {e}")
        finally:
            self._close_db()  # Ensure connection is closed after operations.

    def _create_tables(self):
        """
        Creates necessary tables in the DuckDB database if they do not already exist.
        """
        try:
            if self.conn:
                # Create table for zip code dimension.
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS DimZipCode (
                        GeoID VARCHAR,
                        City TEXT,
                        State TEXT
                    )
                ''')
                
                # Create table for NAICS dimension.
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS DimNaics (
                        NaicsCode VARCHAR,
                        industry_detail TEXT
                    )
                ''')
                
                # Create table for year dimension.
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS DimYear (
                        Year INTEGER,
                        YearDescription TEXT
                    )
                ''')

                # Create table for data entries.
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS DataEntry (
                        EntryID INTEGER,
                        GeoID VARCHAR,
                        NaicsCode VARCHAR,
                        Year INTEGER,
                        Establishments INTEGER,
                        Employees INTEGER,
                        Payroll INTEGER,
                        IndustryLevel INT
                    )
                ''')
        except Exception as e:
            print(f"Error creating tables: {e}")

    def save_to_csv(self):
        """
        Exports data from various tables in the DuckDB database to CSV files in the specified export directory.
        """
        try:
            self._connect_db()  # Connect to the database.
            os.makedirs(self.export_dir, exist_ok=True)  # Create export directory if it doesn't exist.
            
            # Export DimNaics table to CSV.
            self.conn.execute(f"COPY (SELECT * FROM DimNaics) TO '{self.export_dir}/DimNaics.csv' WITH (FORMAT CSV, HEADER TRUE)")
            # Export DimZipCode table to CSV.
            self.conn.execute(f"COPY (SELECT * FROM DimZipCode) TO '{self.export_dir}/DimZipCode.csv' WITH (FORMAT CSV, HEADER TRUE)")
            # Export DimYear table to CSV.
            self.conn.execute(f"COPY (SELECT * FROM DimYear) TO '{self.export_dir}/DimYear.csv' WITH (FORMAT CSV, HEADER TRUE)")

            # Get the list of valid years from DimYear.
            valid_years = self.conn.execute("SELECT Year FROM DimYear").fetchall()
            # Define the known industry levels.
            industry_levels = [2, 4, 6]
            
            # Define maximum file size in bytes (25MB).
            max_file_size = 25 * 1024 * 1024

            # Loop through each valid year and industry level to export the corresponding DataEntry data.
            for year_tuple in tqdm(valid_years, desc="Saving DataEntry CSV files"):
                year = year_tuple[0]  # Extract the year from the tuple.
                for industry_level in industry_levels:
                    # Get the total number of rows for the current year and industry level.
                    total_rows_query = f"""
                    SELECT COUNT(*) FROM DataEntry
                    WHERE Year = {year} AND IndustryLevel = {industry_level}
                    """
                    total_rows = self.conn.execute(total_rows_query).fetchone()[0]

                    if total_rows == 0:
                        continue  # Skip to the next if no rows to export.

                    # Estimate the number of chunks based on the average row size.
                    avg_row_size_query = f"""
                    SELECT AVG(LENGTH(CAST(EntryID AS VARCHAR)) + 
                            LENGTH(CAST(GeoID AS VARCHAR)) + 
                            LENGTH(CAST(NaicsCode AS VARCHAR)) + 
                            LENGTH(CAST(Year AS VARCHAR)) + 
                            LENGTH(CAST(Establishments AS VARCHAR)) + 
                            LENGTH(CAST(Employees AS VARCHAR)) + 
                            LENGTH(CAST(Payroll AS VARCHAR)) + 
                            LENGTH(CAST(IndustryLevel AS VARCHAR))) 
                    FROM DataEntry
                    WHERE Year = {year} AND IndustryLevel = {industry_level}
                    """
                    avg_row_size = self.conn.execute(avg_row_size_query).fetchone()[0]

                    if avg_row_size is None:
                        avg_row_size = 500  # Default average row size if not calculable.

                    rows_per_chunk = math.ceil(max_file_size / avg_row_size)  # Calculate rows per chunk.

                    # Export data in chunks.
                    for start in range(0, total_rows, rows_per_chunk):
                        chunk_file_path = f'{self.export_dir}/DataEntry_{year}_IndustryLevel_{industry_level}_chunk_{start // rows_per_chunk}.csv'
                        
                        # Execute the query and export the data.
                        query = f"""
                        COPY (
                            SELECT * 
                            FROM DataEntry
                            WHERE Year = {year} AND IndustryLevel = {industry_level}
                            LIMIT {rows_per_chunk} OFFSET {start}
                        ) TO '{chunk_file_path}' WITH (FORMAT CSV, HEADER TRUE)
                        """
                        self.conn.execute(query)
        except Exception as e:
            print(f"An error occurred during CSV export: {e}")
        finally:
            self._close_db()  # Ensure connection is closed after operations.


    def import_csv_files(self, table_name):
        """
        Imports CSV files matching the specified table name from the export directory into the DuckDB database.

        Args:
            table_name (str): The name of the table to import data into.
        """
        try:
            self._connect_db()  # Connect to the database.
            
            # Get CSV files with the specified prefix from the export directory.
            csv_files = [f for f in os.listdir(self.export_dir) if f.endswith('.csv') and f.startswith(table_name)]
            if not csv_files:
                print(f"No CSV files found with prefix {table_name}.")
                return
            
            for csv_file in tqdm(csv_files, desc=f"Importing {table_name} CSV file(s)"):
                csv_file_path = os.path.join(self.export_dir, csv_file)  # Construct the full file path.
                try:
                    # Insert data from the CSV file into the specified table.
                    self.conn.execute(f'''
                    INSERT INTO {table_name}
                    SELECT * FROM read_csv_auto('{csv_file_path}')
                    ''')
                except Exception as e:
                    print(f"Failed to import {csv_file}: {e}")  # Log any errors encountered during import.
        except Exception as e:
            print(f"An error occurred during the CSV import process: {e}")
        finally:
            self._close_db()  # Ensure the connection is closed after operations.

    def check_row_length(self, tablename):
        """
        Checks the number of rows in the specified table.

        Args:
            tablename (str): The name of the table to check.

        Returns:
            int: The count of rows in the specified table.
        """
        self._connect_db()  # Connect to the database.
        
        query = f"SELECT COUNT(*) FROM {tablename}"  # Query to count rows in the specified table.
        result = self.conn.execute(query).fetchone()  # Execute the query and fetch the result.
        
        self._close_db()  # Close the connection.
        return result[0]  # Return the row count.

    def import_all_csv_files(self):
        """
        Imports all CSV files for predefined tables from the export directory into the DuckDB database.
        The tables to import are DataEntry, DimNaics, DimYear, and DimZipCode.
        """
        try:
            self._connect_db()  # Connect to the database.
            self._create_tables()  # Ensure necessary tables exist.

            tables = ['DataEntry', 'DimNaics', 'DimYear', 'DimZipCode']  # List of tables to import data into.
            for table_name in tables:
                try:
                    self.import_csv_files(table_name)  # Import CSV files for each table.
                except Exception as e:
                    print(f"An error occurred while importing CSV files for {table_name}: {e}")

        except Exception as e:
            print(f"An error occurred during the import process: {e}")
        finally:
            self._close_db()  # Ensure the connection is closed after operations.

    def get_schema(self):
        """
        Retrieves the schema of all tables in the DuckDB database.

        Returns:
            dict: A dictionary where keys are table names and values are lists of tuples containing
                  column names and their data types.
        """
        self._connect_db()  # Connect to the database.

        schema = {}  # Dictionary to store table schemas.
        tables = self.conn.execute("SHOW TABLES").fetchall()  # Fetch all table names.

        for table in tables:
            table_name = table[0]  # Get the name of the table.
            columns = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()  # Get column info for the table.
            schema[table_name] = [(col[1], col[2]) for col in columns]  # Store column name and data type.

        self._close_db()  # Close the connection.
        return schema  # Return the schema information.

    def check_database_exists(self):
        """
        Checks if the DuckDB database file exists.

        Returns:
            bool: True if the database file exists, False otherwise.
        """
        return os.path.exists(self.db_path)  # Return the existence status of the database file.

    def check_csv_files_exist(self, prefix):
        """
        Checks if any CSV files with a specified prefix exist in the export directory.

        Args:
            prefix (str): The prefix to check for in CSV file names.

        Returns:
            bool: True if any CSV files with the specified prefix exist, False otherwise.
        """
        # Get a list of CSV files in the export directory that match the prefix.
        csv_files = [f for f in os.listdir(self.export_dir) if f.endswith('.csv') and f.startswith(prefix)]
        return bool(csv_files)  # Return whether any matching CSV files exist.
