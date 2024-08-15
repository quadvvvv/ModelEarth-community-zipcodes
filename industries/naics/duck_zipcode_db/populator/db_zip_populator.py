import datetime  # Importing the datetime module for date manipulation
import requests as r  # Importing requests library for making HTTP requests
import time  # Importing time module for handling sleep intervals
import database_populator as dbp  # Importing the DatabasePopulator class from the database_populator module
import pandas as pd  # Importing pandas for data manipulation
from tqdm import tqdm  # Importing tqdm for progress bar functionality

class ZipPopulator:
    def __init__(self, industry_levels, db_path, startyear=2012, endyear=None, api_headers=None, separate_databases=False):
        """
        Initialize the ZipPopulator class.

        Parameters:
            industry_levels (any): NAICS levels.
            startyear (int): The starting year for data retrieval.
            endyear (int): The ending year for data retrieval.
            api_headers (dict): Headers for the API requests, including the API key.
            separate_databases (bool): Flag indicating whether to create separate yearly databases.
        """
        self.api_headers = api_headers  # Store API headers
        self.base_url = "https://api.census.gov/data"  # Base URL for the Census API
        self.startyear = startyear  # Set the starting year
        self.endyear = endyear if endyear else datetime.datetime.now().year - 1  # Set the ending year
        self.separate_databases = separate_databases
        self.industry_levels = industry_levels
        self.db_path = db_path 
        self.failed_attempts = set()  # Set to track failed attempts to fetch data



    def get_zip_for_year(self, year):
        """
        Retrieve and insert zip code data for a specific year.

        Parameters:
            year (int): The year for which to retrieve data.
        """
        # Read industries from CSV file into a DataFrame
        industries = pd.read_csv('./id_lists/industry_id_list.csv')

        # Convert NAICS code '0' (representing all sectors) to '00' to maintain consistency
        # with other parts of the code and ensure it is included in the data pull.
        industries['relevant_naics'] = industries['relevant_naics'].astype(int).astype(str).replace({'0': '00'})

        # Calculate the length of each NAICS code (as a string) and store it in a new column 'level'
        industries['level'] = industries['relevant_naics'].apply(len)

        # This retains only NAICS codes with lengths of characters given by the 'industry_levels'
        industries = industries[industries['level'].isin(self.industry_levels)]
        
        # Check if industries DataFrame contains the required column
        if not hasattr(industries, 'get') or 'relevant_naics' not in industries:
            raise ValueError("industries must be a dictionary-like object with a 'relevant_naics' key")

        # Open the database connection, potentially using a new instance for separate databases
        if self.separate_databases:
            with dbp.DatabasePopulator(db_path=self.db_path, year=year, separate_databases=self.separate_databases) as db_populator:
                self.db_populator = db_populator
                for industry in tqdm(industries['relevant_naics'], desc=f"Inserting for year: {year}"):
                    self._get_zip_and_year_help(industry, year)
        else:
            with dbp.DatabasePopulator(db_path=self.db_path, startyear=self.startyear, endyear=self.endyear) as db_populator:
                self.db_populator = db_populator
                for industry in tqdm(industries['relevant_naics'], desc=f"Inserting for year: {year}"):
                    self._get_zip_and_year_help(industry, year)

        # since we are using context manager, this line should be redundant
        if self.db_populator is not None:
            self.db_populator.close()

    def _get_zip_and_year_help(self, industry, year):
        """
        Helper method to fetch and insert zip code data for a specific industry and year.

        Parameters:
            industry (str): The NAICS code of the industry.
            year (int): The year for which to retrieve data.
        """
        if (industry, year) in self.failed_attempts:
            # print(f"Skipping industry data for industry: {industry} year: {year} due to previous failure")
            return

        # print(f"Getting industry data for industry: {industry}\tyear: {year}")
        data, status_code = self._get_response_data(industry, year)
        if status_code == 304:
            return

        if status_code == 204 or status_code != 200:
            # print(f"Failed to fetch data for year {year}: {status_code}")
            self.failed_attempts.add((industry, year))  # Add to failed attempts
            return

        data_excluding_column_names = data[1:]  # Exclude column names from the data
        batch_data = []  # List to store batch data for insertion

        # Process each row of data
        for line in data_excluding_column_names:
            naics_code = str(line[1]).strip()  # Extract the NAICS code
            row = self._create_row(naics_code, line, year)  # Create a row for insertion
            batch_data.append(row)

            # Insert data in batches of 1000
            if len(batch_data) >= 1000:
                self.db_populator.insert_data_entries(batch_data)
                batch_data = []  # Reset the batch data

        # Insert any remaining data
        if batch_data:
            self.db_populator.insert_data_entries(batch_data)

        # print(f"Finished processing {industry} for {year}.")

# Note: The 'get_zip_zbp', 'get_all_zip_zbp', '_escape_and_quote', '_valid_naics_level' methods are defined but 
# not currently invoked in the codebase.
# Consider reviewing the implementation to ensure they are integrated where needed, 
# or remove them if they are unnecessary.

# However, get_zip_zbp is only used in the jupyter notebook

    def get_zip_zbp(self, industry):
        """
        Retrieve and insert zip code data for all years for a specific industry.

        Parameters:
            industry (str): The NAICS code of the industry.
        """
        years = range(self.startyear, self.endyear + 1)  # Create a range of years
        for year in years:
            self._get_zip_and_year_help(industry, year)  # Fetch and insert data for each year

    def get_all_zip_zbp(self):
        """
        Retrieve and insert zip code data for all industries over the specified year range.
        """
        industries = pd.read_csv('./id_lists/industry_id_list.csv')
        industries['relevant_naics'] = industries['relevant_naics'].astype(int).astype(str)
        industries['level'] = industries['relevant_naics'].apply(len)
        industries = industries[industries['level'].isin(self.industry_levels)]
        
        # Check if industries DataFrame contains the required column
        if not hasattr(industries, 'get') or 'relevant_naics' not in industries:
            raise ValueError("industries must be a dictionary-like object with a 'relevant_naics' key")

        self.db_populator.open()  # Open the database connection

        # Iterate over each relevant NAICS code
        for industry in industries['relevant_naics']:
            if industry == 0:
                self.get_zip_zbp('00')  # Handle the '00' NAICS code
            else:
                self.get_zip_zbp(industry)  # Fetch and insert data for the industry

        self.db_populator.close()  # Close the database connection

    def _escape_and_quote(self, item):
        """
        Escape and quote a string for CSV output.

        Parameters:
            item (str): The string to escape and quote.

        Returns:
            str: The escaped and quoted string.
        """
        if item is None or item == '':
            return '"0"'  # Return a placeholder for None or empty strings
        escaped_item = item.replace('"', '""')  # Escape double quotes
        return f'"{escaped_item}"'  # Return the quoted string

    def _naics_year_selector(self, year):
        """
        Helper method for _get_response_data.

        Select the appropriate NAICS code version based on the year.

        Parameters:
            year (int): The year to evaluate.

        Returns:
            str: The corresponding NAICS code version.
        """
        if year >= 2000 and year <= 2002:
            return "NAICS1997"
        elif year >= 2003 and year <= 2007:
            return "NAICS2002"
        elif year >= 2008 and year <= 2011:
            return "NAICS2007"
        elif year >= 2012 and year <= 2016:
            return "NAICS2012"
        return "NAICS2017"  # Default to the latest version

    def _valid_naics_level(self, naics_code):
        """
        Validate the level of the NAICS code.

        Parameters:
            naics_code (str): The NAICS code to validate.

        Returns:
            int: The length of the NAICS code or its parts if it's a range.
        """
        if '-' in naics_code:  # Check for NAICS code ranges
            parts = naics_code.split('-')
            if all(part.isdigit() for part in parts):  # Ensure all parts are digits
                return len(parts[0])  # Return length of the first part
        return len(naics_code)  # Return the length of the code

    def _create_row(self, naics_code, line, year):
        """
        Helper method for _get_zip_and_year_help.

        Create a data row for insertion into the database.

        Parameters:
            naics_code (str): The NAICS code of the industry.
            line (list): The data line containing various fields.
            year (int): The year of the data.

        Returns:
            list: A list representing a data row.
        """
        return [
            line[0],  # GeoID
            naics_code,  # NAICS code
            year,  # Year
            line[2],  # Establishments
            line[3],  # Employees
            line[4],  # Payroll
            len(naics_code)  # Industry level (length of the NAICS code)
        ]

    def _get_response_data(self, sector, year, attempt=1):
        """
        Helper method for _get_zip_and_year_help.

        Fetch data from the API for a specific sector and year.

        Parameters:
            sector (str): The NAICS sector code.
            year (int): The year for which to fetch data.
            attempt (int): The current attempt number for retries (default is 1).

        Returns:
            tuple: A tuple containing the fetched data and the HTTP status code.
        """
        # Check if the data for the given sector and year already exists in the database
        if self.db_populator.data_for_year_and_sector_exists(year, sector):
            # If data exists, skip the API call and return a 304 status
            # print(f"Data for sector {sector} and year {year} already exists in the database. Skipping API call.")
            return {}, 304

        # Select the appropriate NAICS code based on the year
        code = self._naics_year_selector(year)

        # Ensure the sector is a string
        if not isinstance(sector, str):
            sector = str(sector)

        # Standardize the sector code if it's '0'
        if sector == '0':
            sector = '00'

        # Construct the API request URL
        url = f"{self.base_url}/{year}/zbp?get=ZIPCODE,{code},ESTAB,EMP,PAYANN&for=zip%20code:*&{code}={sector}&key={self.api_headers['x-api-key']}"

        # Use a different endpoint for years greater than 2018
        if year > 2018:
            url = f"{self.base_url}/{year}/cbp?get=ZIPCODE,{code},ESTAB,EMP,PAYANN&for=zip%20code:*&{code}={sector}&key={self.api_headers['x-api-key']}"

        # Make the API request
        response = r.get(url, headers=self.api_headers)

        # Handle rate limiting by retrying the request
        if response.status_code == 429:
            if attempt <= 5:  # Retry up to 5 times
                sleep_time = (2 ** attempt)  # Exponential backoff
                print(f"Rate limit exceeded, retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
                return self._get_response_data(sector, year, attempt + 1)
            else:
                print("Max retry attempts reached, unable to retrieve data.")
                return {}, 429

        # Check for a successful response
        if response.status_code == 200:
            data = response.json()  # Parse the JSON data
            return data, 200  # Return the data and status code
        else:
            # print(f"Failed to fetch data: {response.status_code}")
            return {}, response.status_code  # Return an empty dict and status code for errors

    def close_resources(self):
        """
        Close the database resources if they are open.
        """
        if self.db_populator:
            self.db_populator.close()  # Close the database connection


