import os
import logging
import duckdb
from multiprocessing import Pool
from tqdm import tqdm
import query as q

class DataExporter:
    def __init__(self, base_db_path='../zip_data/duck_db_manager/database/', threads=4, export_dir='community-zipcodes', industry_levels=[2, 4, 6], year=None):
        """
        Initializes the DataExporter with the given database path, number of threads, export directory, industry levels, and year.

        Args:
            base_db_path (str): Path to the base DuckDB database directory (default is '../zip_data/duck_db_manager/database/').
            threads (int): Number of threads for data export (default is 4).
            export_dir (str): Directory to save the exported CSV files (default is 'community-zipcodes').
            industry_levels (list): List of NAICS industry levels to use for data export (default is [2, 4, 6]).
            year (int): The year of the database to use for data export.
        """
        self.export_dir = export_dir
        os.makedirs(self.export_dir, exist_ok=True)
        self.base_db_path = base_db_path
        self.threads = threads
        self.industry_levels = industry_levels
        self.year = year
        self.qm = q.DataQueryManager(db_path=self._get_db_path_for_year(year))
        logging.basicConfig(level=logging.INFO)

    def _get_db_path_for_year(self, year):
        """
        Constructs the database path for the given year.

        Args:
            year (int): The year of the database file.

        Returns:
            str: Path to the DuckDB database file for the specified year.

        Raises:
            ValueError: If year is not specified.
        """
        if year is None:
            raise ValueError("Year must be specified.")
        return os.path.join(self.base_db_path, f'us_naics_census_data_{year}.duckdb')

    def _fetch_geo_ids(self):
        """
        Fetches all unique geographic IDs (ZIP codes) from the DimZipCode table.

        Returns:
            list: A list of unique ZIP codes.
        """
        with duckdb.connect(self.qm.db_path, read_only=True) as conn:
            try:
                geo_ids = conn.execute("SELECT DISTINCT GeoID FROM DimZipCode").fetchall()
                return [geo_id[0] for geo_id in geo_ids]
            except Exception as e:
                logging.error(f"Failed to fetch GeoIDs: {e}")
                return []

    def _fetch_state_for_zipcode(self, zipcode):
        """
        Fetches the state corresponding to a given ZIP code from the DimZipCode table.

        Args:
            zipcode (str): The ZIP code for which the state is to be fetched.

        Returns:
            str: The state corresponding to the ZIP code, or None if not found.
        """
        with duckdb.connect(self.qm.db_path, read_only=True) as conn:
            try:
                state = conn.execute("SELECT State FROM DimZipCode WHERE GeoID = ?", (zipcode,)).fetchone()
                return state[0] if state else None
            except Exception as e:
                logging.error(f"Failed to fetch state for ZIP code {zipcode}: {e}")
                return None

    def make_csv(self, zipcode, industry_level=None):
        """
        Generates a CSV file for the given ZIP code and industry level.

        Args:
            zipcode (str): The ZIP code for which data is to be exported.
            industry_level (int, optional): The NAICS industry level for which data is to be exported.

        Returns:
            str: The file path of the generated CSV file, or None if the state could not be determined.
        """
        if not (zipcode.isdigit() and len(zipcode) == 5):
            raise ValueError("Zipcode must be a string of exactly 5 digits.")

        state = self._fetch_state_for_zipcode(zipcode)
        if not state:
            logging.error(f"State not found for ZIP code {zipcode}. Skipping...")
            return None
        
        # Construct path
        nested_path = os.path.join(self.export_dir, 'industries', 'naics', 'US', 'zip', state)
        os.makedirs(nested_path, exist_ok=True)
        
        # Construct filename
        filename_parts = [f'US-{state}']
        if industry_level:
            filename_parts.append(f'census-naics{industry_level}')
        filename_parts.append(f'zip-{self.year}')
        filename = "-".join(filename_parts) + ".csv"
        filepath = os.path.join(nested_path, filename)
        
        with duckdb.connect(self.qm.db_path, read_only=True) as conn:
            results = self.qm.filter(zipcode=zipcode, year=self.year, industry_level=industry_level, conn=conn)
            results.to_csv(filepath, index=False)
        
        return filepath

    def worker(self, args):
        """
        Worker function for multiprocessing to handle CSV generation for each combination of arguments.

        Args:
            args (tuple): A tuple containing (zipcode, industry_level).

        Returns:
            str: The file path of the generated CSV file.
        """
        zipcode, level = args
        return self.make_csv(zipcode, industry_level=level)

    def export_all_geo_data(self):
        """
        Exports data for all ZIP codes and industry levels using multiprocessing.

        Returns:
            list: A list of file paths for the exported CSV files.
        """
        geo_ids = self._fetch_geo_ids()
        levels = self.industry_levels
        all_combinations = [(geo_id, level) for geo_id in geo_ids for level in levels]

        try:
            with Pool(processes=self.threads) as pool:
                results = list(tqdm(pool.imap(self.worker, all_combinations), total=len(all_combinations), desc="Exporting data"))
                return results
        except KeyboardInterrupt:
            print("Interrupted by user. Exiting...")
            pool.terminate()
            pool.join()
            logging.info("Exporting process was interrupted by user.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    def debug_export_all_geo_data(self):
        """
        Debug method to export data for all ZIP codes and industry levels without multiprocessing.

        This method is useful for debugging and validating the export process.

        Returns:
            None
        """
        geo_ids = self._fetch_geo_ids()

        for geo_id in tqdm(geo_ids, desc="GeoID Export"):
            for level in tqdm(self.industry_levels, desc="Industry Level Export", leave=False):
                try:
                    self.make_csv(geo_id, industry_level=level)
                except Exception as e:
                    logging.error(f"Failed to export data for GeoID {geo_id} and Industry Level {level}: {e}")
