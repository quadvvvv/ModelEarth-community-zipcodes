import duckdb  # Import DuckDB for database management
import pandas as pd  # Import pandas for data manipulation
import requests as r  # Import requests for HTTP requests
import re  # Import regular expressions for string matching
from tqdm import tqdm  # Import tqdm for progress bars
import datetime  # Import datetime for date handling
import os  # Import os for operating system functions

class DatabasePopulator:
    def __init__(self, year=None, separate_databases=False, db_path='../zip_data/duck_db_manager/database/us_census_nacis_data', startyear=2012, endyear=None):
        """
        Initializes the DatabasePopulator instance.

        Parameters:
            year (int, optional): The year for which to create the database. If None and separate_databases is True, a single database for all years is created.
            separate_databases (bool): Flag to determine if separate databases should be created for each year.
            db_path (str): The base path for the DuckDB database.
            startyear (int): The starting year for data population.
            endyear (int): The ending year for data population. Defaults to the current year minus one.
        """
        self.db_path = db_path  # Store the base database path
        self.conn = None  # Initialize the database connection
        self.startyear = startyear  # Store the starting year
        self.endyear = endyear if endyear else datetime.datetime.now().year - 1  # Set the ending year
        self.year = year  # Store the specific year
        self.separate_databases = separate_databases  # Store the flag for separate databases

    def __enter__(self):
        """Establish a database connection and create necessary tables."""
        if self.separate_databases and self.year is not None:
            db_path = f"{self.db_path}_{self.year}.duckdb"  # Separate databases for each year
        else:
            db_path = f"{self.db_path}.duckdb"  # Single database for all years

        self.conn = duckdb.connect(db_path)  # Establish the database connection
        self._create_tables()  # Create required tables in the database
        self.populate_dim_zipcode()  # Populate the DimZipCode table
        self.populate_naics()  # Populate the DimNaics table
        self.populate_year()  # Populate the DimYear table
        return self  # Return the instance for use in a context manager
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the database connection when exiting the context manager."""
        self.close()  # Close the database connection

    def open(self):
        """Open the database connection if it is not already open."""
        if self.conn is None:
            db_path = f"{self.db_path}_{self.year}.duckdb" if self.separate_databases and self.year is not None else f"{self.db_path}.duckdb"
            self.conn = duckdb.connect(db_path)  # Connect to the DuckDB database

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()  # Close the connection
            self.conn = None  # Set the connection to None

    def generate_entry_ids(self, num_rows):
        """
        Generate entry IDs for the specified number of rows.

        Parameters:
            num_rows (int): The number of rows for which to generate IDs.

        Returns:
            list: A list of generated entry IDs.
        """
        next_ids = self.conn.execute(f"SELECT nextval('entryid_seq') FROM range(0, {num_rows})").fetchall()  # Fetch next entry IDs from the sequence
        return [row[0] for row in next_ids]  # Return the list of entry IDs

    def insert_data_entries(self, data_rows):
        """
        Insert data entries into the DataEntry table.

        Parameters:
            data_rows (list): A list of data rows to insert.
        """
        try:
            num_rows = len(data_rows)  # Get the number of rows to insert
            entry_ids = self.generate_entry_ids(num_rows)  # Generate entry IDs for new rows

            # Combine entry IDs with data rows
            data_rows_with_id = [
                (entry_id,) + tuple(row)  # Create a tuple with entry ID and row data
                for entry_id, row in zip(entry_ids, data_rows)
            ]

            # Define columns for the DataFrame
            columns = ['EntryID', 'GeoID', 'NaicsCode', 'Year', 'Establishments', 'Employees', 'Payroll', 'IndustryLevel']
            df = pd.DataFrame(data_rows_with_id, columns=columns)  # Create a DataFrame from the data

            # Save the DataFrame to a temporary CSV file
            # TODO: Github Action have to set relative path for this to work
            df.to_csv('../temp/data_entries_temp.csv', index=False)

            # Load the data from the CSV file into the DataEntry table
            self.conn.execute("COPY DataEntry FROM '../temp/data_entries_temp.csv' (AUTO_DETECT TRUE)")

        except duckdb.Error as e:
            print(f"An error occurred: {e}")  # Print error message if an exception occurs

    def data_for_year_and_sector_exists(self, year, sector):
        """
        Check if data exists for a specific year and sector.

        Parameters:
            year (int): The year to check.
            sector (str): The NAICS code of the sector to check.

        Returns:
            bool: True if data exists, False otherwise.
        """
        query = '''
            SELECT EXISTS(
                SELECT 1 FROM DataEntry WHERE Year = ? AND NaicsCode = ?
            )
        '''
        result = self.conn.execute(query, (year, sector)).fetchone()  # Execute query to check existence
        return result[0] == 1 if result else False  # Return True if data exists

    def data_exists(self, table_name):
        """
        Check if any data exists in the specified table.

        Parameters:
            table_name (str): The name of the table to check.

        Returns:
            bool: True if data exists, False otherwise.
        """
        query = f"SELECT EXISTS(SELECT 1 FROM {table_name} LIMIT 1)"  # Query to check if table has data
        result = self.conn.execute(query).fetchone()  # Execute query
        return result[0] == 1 if result else False  # Return True if data exists

    def _create_tables(self):
        """Create necessary tables in the database if they do not already exist."""
        try:
            self.conn.execute("CREATE SEQUENCE entryid_seq")  # Create sequence for entry IDs
        except duckdb.CatalogException:
            pass  # Ignore if sequence already exists

        temp_dir = '../temp'  # Define temporary directory for CSV files

        # Check if the directory already exists
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)  # Create the temporary directory if it does not exist
        
        # Create DimZipCode table
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS DimZipCode (
                GeoID VARCHAR,
                City TEXT,
                State TEXT
            )
        ''')
        
        # Create DimNaics table
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS DimNaics (
                NaicsCode VARCHAR,
                industry_detail TEXT
            )
        ''')
        
        # Create DimYear table
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS DimYear (
                Year INTEGER,
                YearDescription TEXT
            )
        ''')

        # Create DataEntry table
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
    
    def get_all_zipcode_names(self, year):
        """
        Retrieve all zip code names for the specified year from the Census API.

        Parameters:
            year (int): The year for which to retrieve zip code names.

        Returns:
            list: A list of tuples containing geo ID and geo name.
        """
        url = f"https://api.census.gov/data/{year}/zbp?get=GEO_TTL&for=zip%20code:*&key=975f39a54e48438ceebf303d6018e34db212e804"
        
        # Adjust URL for specific years
        if int(year) == 2017 or int(year) == 2018:
            url = f"https://api.census.gov/data/{year}/zbp?get=NAME&for=zip%20code:*&key=975f39a54e48438ceebf303d6018e34db212e804"
        if int(year) > 2018:
            url = f"https://api.census.gov/data/{year}/cbp?get=NAME&for=zip%20code:*&key=975f39a54e48438ceebf303d6018e34db212e804"
        
        response = r.get(url)  # Make a GET request to the API
        if response.status_code == 200:
            data = response.json()  # Parse the JSON response
            return [(row[1], row[0]) for row in data[1:]]  # Return list of geo ID and geo name tuples
        else:
            #print(f"Failed to fetch data for year {year}: {response.status_code}")
            return []  # Return an empty list if the request fails

    def extract_city_state(self, geo_name):
        """
        Extract city and state from a geo name string.

        Parameters:
            geo_name (str): The geo name string containing city and state.

        Returns:
            tuple: A tuple containing the city and state, or (None, None) if extraction fails.
        """
        match = re.match(r'ZIP \d+ \((.+), (.+)\)', geo_name)  # Use regex to match the city and state
        if match:
            return match.group(1), match.group(2)  # Return extracted city and state
        else:
            return None, None  # Return None if extraction fails


    def populate_dim_zipcode(self):
        """Populate the DimZipCode table with unique zip code data."""
        if self.data_exists('DimZipCode'):
            return  # Exit if DimZipCode table already has data
        
        unique_zip_code_data = set()  # Initialize a set to store unique zip code data
        # Iterate through years from 2012 to the current year minus one
        for year in tqdm(range(2012, datetime.datetime.now().year - 1), desc="Fetching zip code names"):
            zip_code_data = self.get_all_zipcode_names(str(year))  # Fetch zip code data for the year
            for geo_id, geo_name in zip_code_data:
                city, state = self.extract_city_state(geo_name)  # Extract city and state from geo name
                if city and state:  # Check if both city and state were extracted successfully
                    unique_zip_code_data.add((geo_id, city, state))  # Add unique zip code data to the set

        # Define columns for the DataFrame
        columns = ['GeoID', 'City', 'State']
        df = pd.DataFrame(list(unique_zip_code_data), columns=columns)  # Create a DataFrame from the unique data

        # Save the DataFrame to a temporary CSV file
        df.to_csv('../temp/zip_code_temp.csv', index=False)
        # Load the data from the CSV file into the DimZipCode table
        self.conn.execute("COPY DimZipCode FROM '../temp/zip_code_temp.csv' (AUTO_DETECT TRUE)")

        # Insert a default row into DimZipCode for missing zip code information
        sql = """
            INSERT INTO DimZipCode (GeoID, City, State)
            VALUES (?, ?, ?)
        """
        self.conn.execute(sql, ['99999', None, None])  # Insert a placeholder row

    def populate_naics(self):
        """Populate the DimNaics table with industry data from a CSV file."""
        if self.data_exists('DimNaics'):
            return  # Exit if DimNaics table already has data
        
        df = pd.read_csv('id_lists/industry_id_list.csv')  # Read industry data from CSV

        # Function to format NAICS codes
        def format_naics(value):
            if value == 0.0:
                return '00'  # Return '00' for zero value
            else:
                return str(int(value)).zfill(2)  # Format NAICS code with leading zeros

        df['relevant_naics'] = df['relevant_naics'].apply(format_naics)  # Apply formatting to NAICS codes

        # Insert formatted NAICS data into the DimNaics table
        for _, row in tqdm(df.iterrows(), desc="Fetching NAICS"):
            self.conn.execute('INSERT INTO DimNaics (NaicsCode, industry_detail) VALUES (?, ?)', (row['relevant_naics'], row['industry_detail']))

    def populate_year(self):  
        """Populate the DimYear table with year data and corresponding NAICS versions."""
        if self.data_exists('DimYear'):
            return  # Exit if DimYear table already has data
        
        # Function to select the appropriate NAICS version based on the year
        def _naics_year_selector(year):
            if year >= 2000 and year <= 2002:
                return "NAICS1997"  # Return NAICS version for years 2000-2002
            elif year >= 2003 and year <= 2007:
                return "NAICS2002"  # Return NAICS version for years 2003-2007
            elif year >= 2008 and year <= 2011:
                return "NAICS2007"  # Return NAICS version for years 2008-2011
            elif year >= 2012 and year <= 2016:
                return "NAICS2012"  # Return NAICS version for years 2012-2016
            return "NAICS2017"  # Default to NAICS2017 for other years

        # Create a list of tuples containing year and its corresponding NAICS version
        years_data = [(year, _naics_year_selector(year)) for year in tqdm(range(self.startyear, self.endyear + 1), desc="Fetching years")]

        # Insert year data into the DimYear table
        self.conn.executemany('INSERT INTO DimYear (Year, YearDescription) VALUES (?, ?)', years_data)
