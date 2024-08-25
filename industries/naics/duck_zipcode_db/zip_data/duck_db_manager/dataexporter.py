import os
import logging
import duckdb
from multiprocessing import Pool
from tqdm import tqdm
import query as q
import re

class DataExporter:
    """
    A class to export economic data related to geographic areas and years 
    into CSV files from a DuckDB database. This class is *deprecated* but 
    retained for reference or future use.
    
    Attributes:
        db_path (str): The path to the DuckDB database.
        threads (int): The number of threads to use for exporting data.
        export_dir (str): The directory where the exported CSV files will be saved.
        qm (DataQueryManager): An instance of the DataQueryManager for querying data.
    """

    def __init__(self, db_path='database/us_economic_data.duckdb', threads=4, export_dir='zip'):
        """
        Initializes the DataExporter with the given database path, number of threads, and export directory.

        Args:
            db_path (str): Path to the DuckDB database (default is 'database/us_economic_data.duckdb').
            threads (int): Number of threads for data export (default is 4).
            export_dir (str): Directory to save the exported CSV files (default is 'zip').
        """
        self.export_dir = export_dir
        os.makedirs(self.export_dir, exist_ok=True)  # Create the export directory if it does not exist
        self.db_path = db_path
        self.threads = threads
        self.qm = q.DataQueryManager(db_path=self.db_path, export_dir=self.export_dir)  # Initialize the query manager
        logging.basicConfig(level=logging.INFO)  # Set up logging configuration

    def _fetch_geo_ids(self):
        """
        Fetches unique GeoIDs from the database that are present in the DataEntry table.

        Returns:
            list: A list of GeoIDs or an empty list if fetching fails.
        """
        with duckdb.connect(self.db_path, read_only=True) as conn:
            try:
                geo_ids = conn.execute("""
                    SELECT GeoID 
                    FROM DimZipCode 
                    WHERE GeoID IN (SELECT DISTINCT GeoID FROM DataEntry)
                """).fetchall()
                return [geo_id[0] for geo_id in geo_ids]  # Extract the first element of each tuple
            except Exception as e:
                logging.error(f"Failed to fetch GeoIDs: {e}")
                return []

    def _fetch_years(self):
        """
        Fetches unique years from the database that are present in the DataEntry table.

        Returns:
            list: A list of years or an empty list if fetching fails.
        """
        with duckdb.connect(self.db_path, read_only=True) as conn:
            try:
                years = conn.execute("""
                    SELECT Year 
                    FROM DimYear 
                    WHERE Year IN (SELECT DISTINCT Year FROM DataEntry)
                """).fetchall()
                return [year[0] for year in years]  # Extract the first element of each tuple
            except Exception as e:
                logging.error(f"Failed to fetch Years: {e}")
                return []

    def make_csv(self, zipcode, year=None, industry_level=None):
        """
        Generates a CSV file for the specified zipcode, year, and industry level.

        The CSV files are exported to a nested directory structure based on the 
        zipcode, where each digit of the zipcode corresponds to a folder level.
        For example, for a zipcode of '30318', the directory structure will be 
        'zip/3/0/3/1/8'.

        Args:
            zipcode (str): The 5-digit zipcode to generate data for.
            year (int, optional): The year of data to export.
            industry_level (int, optional): The NAICS industry level for the data.

        Returns:
            str: The path to the created CSV file.

        Raises:
            ValueError: If the zipcode is not a valid 5-digit string.
        """
        if not (zipcode.isdigit() and len(zipcode) == 5):
            raise ValueError("Zipcode must be a string of exactly 5 digits.")
        
        nested_path = os.path.join(self.export_dir, *list(zipcode))  # Create a nested directory structure
        os.makedirs(nested_path, exist_ok=True)  # Create the directory if it does not exist

        # Construct the filename based on the provided parameters
        filename_parts = ['US', zipcode]
        if industry_level:
            filename_parts.append(f'census-naics{industry_level}')
        if year:
            filename_parts.append(f'zipcode-{year}')
        filename = "-".join(filename_parts) + ".csv"
        filepath = os.path.join(nested_path, filename)

        with duckdb.connect(self.db_path, read_only=True) as conn:
            results = self.qm.filter(zipcode=zipcode, year=year, industry_level=industry_level, conn=conn)
            results.to_csv(filepath, index=False)  # Export results to CSV file
        
        return filepath

    def worker(self, args):
        """
        A worker function for multiprocessing that calls the make_csv method.

        Args:
            args (tuple): A tuple containing geo_id, year, and industry level.

        Returns:
            str: The path to the created CSV file.
        """
        geo_id, year, level = args
        return self.make_csv(geo_id, year=year, industry_level=level)

    def export_all_geo_year_data(self):
        """
        Exports data for all combinations of GeoIDs, years, and industry levels 
        using multiprocessing.

        Returns:
            list: A list of file paths for the exported CSV files or None if interrupted.
        """
        geo_ids = self._fetch_geo_ids()  # Fetch all GeoIDs
        years = self._fetch_years()  # Fetch all years
        levels = [2, 4, 6]  # Define the industry levels to export
        all_combinations = [(geo_id, year, level) for geo_id in geo_ids for year in years for level in levels]  # Generate all combinations

        try:
            with Pool(processes=self.threads) as pool:
                results = list(tqdm(pool.imap(self.worker, all_combinations), total=len(all_combinations), desc="Exporting data"))
                return results
        except KeyboardInterrupt:
            print("Interrupted by user. Exiting...")
            pool.terminate()  # Terminate the pool of workers
            pool.join()  # Wait for the workers to finish
            logging.info("Exporting process was interrupted by user.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    def export_geo_year_data(self, year):
        """
        Exports data for all GeoIDs for a specified year using multiprocessing.

        Args:
            year (int): The year for which to export data.

        Returns:
            list: A list of file paths for the exported CSV files or None if interrupted.
        """
        geo_ids = self._fetch_geo_ids()  # Fetch all GeoIDs
        years = [year]  # Create a list containing the specified year
        levels = [2, 4, 6]  # Define the industry levels to export
        all_combinations = [(geo_id, year, level) for geo_id in geo_ids for level in levels]  # Generate combinations for the specified year

        try:
            with Pool(processes=self.threads) as pool:
                results = list(tqdm(pool.imap(self.worker, all_combinations), total=len(all_combinations), desc="Exporting data"))
                return results
        except KeyboardInterrupt:
            print("Interrupted by user. Exiting...")
            pool.terminate()  # Terminate the pool of workers
            pool.join()  # Wait for the workers to finish
            logging.info("Exporting process was interrupted by user.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    def debug_export_all_geo_year_data(self):
        """
        Exports data for all GeoIDs and years, with error handling 
        to log any failures without interrupting the process.

        This method is intended for debugging purposes.

        Returns:
            None
        """
        geo_ids = self._fetch_geo_ids()  # Fetch all GeoIDs
        years = self._fetch_years()  # Fetch all years

        for geo_id in tqdm(geo_ids, desc="GeoID Export"):
            for year in tqdm(years, desc="Year Export", leave=False):
                try:
                    self.make_csv(geo_id, year=year)  # Attempt to create the CSV file
                except Exception as e:
                    logging.error(f"Failed to export data for GeoID {geo_id} and Year {year}: {e}")
