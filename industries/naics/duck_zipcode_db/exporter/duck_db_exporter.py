import os
import logging
import duckdb
import pandas as pd
from multiprocessing import Pool
import query as q

class DataExporter:
    def __init__(self, base_db_path='../zip_data/duck_db_manager/database/', threads=4, export_dir='../../US/zip', industry_levels=[2, 5, 6], year=None):
        """
        Initializes the DataExporter with the given parameters.

        Args:
            base_db_path (str): Path to the base DuckDB database directory (default is '../zip_data/duck_db_manager/database/').
            threads (int): Number of threads for data export (default is 4).
            export_dir (str): Directory to save the exported CSV files, relative to the script's directory (default is '../../US/zip').
            industry_levels (list): List of NAICS industry levels to use for data export (default is [2, 5, 6]).
            year (int): The year of the database to use for data export.
        """
        self.export_dir = export_dir
        # Create the export directory relative to the script location
        self.absolute_export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.export_dir)
        os.makedirs(self.absolute_export_dir, exist_ok=True)
        self.base_db_path = base_db_path
        self.threads = threads
        self.industry_levels = industry_levels
        self.year = year
        if year is None:
            raise ValueError("Year must be specified.")
        self.qm = q.DataQueryManager(db_path=self._get_db_path_for_year(year), export_dir=self.absolute_export_dir)
        logging.basicConfig(level=logging.INFO)

    def _get_db_path_for_year(self, year):
        """
        Constructs the database path for the specified year.

        Args:
            year (int): The year for which to get the database path.

        Returns:
            str: The full path to the DuckDB database for the specified year.
        """
        return os.path.join(self.base_db_path, f'us_naics_census_data_{year}.duckdb')

    def _fetch_states(self):
        """
        Fetches all unique states from the DimZipCode table.

        Returns:
            list: A list of unique state abbreviations.
        """
        query = "SELECT DISTINCT State FROM DimZipCode"
        with duckdb.connect(self.qm.db_path, read_only=True) as conn:
            try:
                states = conn.execute(query).fetchall()
                return [state[0] for state in states]
            except Exception as e:
                logging.error(f"Failed to fetch states: {e}")
                return []

    def _fetch_zipcodes_for_state(self, state):
        """
        Fetches all ZIP codes for a given state.

        Args:
            state (str): The state for which to get ZIP codes.

        Returns:
            list: A list of ZIP codes for the specified state.
        """
        query = "SELECT GeoID FROM DimZipCode WHERE State = ?"
        with duckdb.connect(self.qm.db_path, read_only=True) as conn:
            try:
                zipcodes = conn.execute(query, (state,)).fetchall()
                return [zipcode[0] for zipcode in zipcodes]
            except Exception as e:
                logging.error(f"Failed to fetch ZIP codes for state {state}: {e}")
                return []

    def make_csv(self, state=None):
        """
        Generates CSV files for all states if no state is specified.
        If `state` is None, it generates a file for unspecified states with a placeholder ZIP code.

        Args:
            state (str, optional): The state for which data is to be exported.

        Returns:
            list: A list of file paths for the generated CSV files.
            dict: A dictionary where keys are file paths and values are the number of rows exported.
        """
        file_paths = []
        states = [state] if state else self._fetch_states()
        industry_level_row_counts = {level: 0 for level in self.industry_levels}
        total_rows_exported = 0

        for state in states:
            if state is None:
                state = "NotSpecified"
                zipcodes = ['99999']
                state_dir = os.path.join(self.absolute_export_dir, "NotSpecified")
            else:
                if not self.absolute_export_dir:
                    raise ValueError("Export directory path is not set.")
                if not state:
                    raise ValueError("State is None or empty.")
                state_dir = os.path.join(self.absolute_export_dir, state)
                os.makedirs(state_dir, exist_ok=True)
                zipcodes = self._fetch_zipcodes_for_state(state)
                if not zipcodes:
                    logging.warning(f"No ZIP codes found for state {state}. Skipping export.")
                    continue

            os.makedirs(state_dir, exist_ok=True)

            for industry_level in self.industry_levels:
                filename = f'US-{state}-census-naics{industry_level}-zip-{self.year}.csv'
                filepath = os.path.join(state_dir, filename)

                zipcodes_str = ', '.join(f"'{zipcode}'" for zipcode in zipcodes)
                data_query = f"""
                SELECT GeoID AS Zipcode, NaicsCode, Establishments, Employees, Payroll
                FROM DataEntry
                WHERE GeoID IN ({zipcodes_str}) 
                AND Year = {self.year} 
                AND IndustryLevel = {industry_level}
                ORDER BY GeoID
                """
                
                with duckdb.connect(self.qm.db_path, read_only=True) as conn:
                    try:
                        results = conn.execute(data_query).fetchdf()
                        if isinstance(results, pd.DataFrame):
                            results.to_csv(filepath, index=False)
                            num_rows = len(results)
                            industry_level_row_counts[industry_level] += num_rows
                            total_rows_exported += num_rows
                            logging.info(f"Export for state {state} at industry level {industry_level} is finished. {num_rows} rows have been exported.")
                        else:
                            logging.error(f"Query result for state {state} at industry level {industry_level} is not a DataFrame.")
                    except Exception as e:
                        logging.error(f"Failed to export data for state {state}, industry level {industry_level}: {e}")

        logging.info(f"Total rows exported for Year {self.year}: {total_rows_exported}")
        return industry_level_row_counts, total_rows_exported

    def worker(self, args):
        """
        Worker function for multiprocessing to generate CSV files for a given state.

        Args:
            args (tuple): A tuple containing the state.

        Returns:
            list: A list of file paths for the generated CSV files.
        """
        state = args
        return self.make_csv(state)
