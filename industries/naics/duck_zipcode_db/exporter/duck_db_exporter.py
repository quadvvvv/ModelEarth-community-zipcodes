import os
import logging
import duckdb
from multiprocessing import Pool
from tqdm import tqdm
import query as q

class DataExporter:
    def __init__(self, db_path='database/us_economic_data.duckdb', threads=4, export_dir='community-zipcodes', industry_levels=[2, 4, 6]):
        """
        Initializes the DataExporter with the given database path, number of threads, export directory, and industry levels.

        Args:
            db_path (str): Path to the DuckDB database (default is 'database/us_economic_data.duckdb').
            threads (int): Number of threads for data export (default is 4).
            export_dir (str): Directory to save the exported CSV files (default is 'community-zipcodes').
            industry_levels (list): List of NAICS industry levels to use for data export (default is [2, 4, 6]).
        """
        self.export_dir = export_dir
        os.makedirs(self.export_dir, exist_ok=True)
        self.db_path = db_path
        self.threads = threads
        self.industry_levels = industry_levels
        self.qm = q.DataQueryManager(db_path=self.db_path)
        logging.basicConfig(level=logging.INFO)

    def _fetch_geo_ids(self):
        with duckdb.connect(self.db_path, read_only=True) as conn:
            try:
                geo_ids = conn.execute("SELECT DISTINCT GeoID FROM DimZipCode").fetchall()
                return [geo_id[0] for geo_id in geo_ids]
            except Exception as e:
                logging.error(f"Failed to fetch GeoIDs: {e}")
                return []

    def _fetch_years(self):
        with duckdb.connect(self.db_path, read_only=True) as conn:
            try:
                years = conn.execute("SELECT DISTINCT Year FROM DimYear").fetchall()
                return [year[0] for year in years]
            except Exception as e:
                logging.error(f"Failed to fetch Years: {e}")
                return []

    def make_csv(self, zipcode, year=None, industry_level=None):
        if not (zipcode.isdigit() and len(zipcode) == 5):
            raise ValueError("Zipcode must be a string of exactly 5 digits.")
        
        # Construct path
        nested_path = os.path.join(self.export_dir, 'industries', 'naics', 'US', 'zip', 'NY')
        os.makedirs(nested_path, exist_ok=True)
        
        # Construct filename
        filename_parts = ['US-NY']
        if industry_level:
            filename_parts.append(f'census-naics{industry_level}')
        if year:
            filename_parts.append(f'zip-{year}')
        filename = "-".join(filename_parts) + ".csv"
        filepath = os.path.join(nested_path, filename)
        
        with duckdb.connect(self.db_path, read_only=True) as conn:
            results = self.qm.filter(zipcode=zipcode, year=year, industry_level=industry_level, conn=conn)
            results.to_csv(filepath, index=False)
        
        return filepath

    def worker(self, args):
        geo_id, year, level = args
        return self.make_csv(geo_id, year=year, industry_level=level)

    def export_all_geo_year_data(self):
        geo_ids = self._fetch_geo_ids()
        years = self._fetch_years()
        levels = self.industry_levels
        all_combinations = [(geo_id, year, level) for geo_id in geo_ids for year in years for level in levels]

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

    def export_geo_year_data(self, year):
        geo_ids = self._fetch_geo_ids()
        levels = self.industry_levels
        all_combinations = [(geo_id, year, level) for geo_id in geo_ids for level in levels]

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

    def debug_export_all_geo_year_data(self):
        geo_ids = self._fetch_geo_ids()
        years = self._fetch_years()

        for geo_id in tqdm(geo_ids, desc="GeoID Export"):
            for year in tqdm(years, desc="Year Export", leave=False):
                try:
                    self.make_csv(geo_id, year=year)
                except Exception as e:
                    logging.error(f"Failed to export data for GeoID {geo_id} and Year {year}: {e}")
