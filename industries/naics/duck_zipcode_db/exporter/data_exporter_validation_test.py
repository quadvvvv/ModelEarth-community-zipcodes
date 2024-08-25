import os
import pandas as pd
import duckdb
import logging
from industries.naics.duck_zipcode_db.exporter.duck_db_exporter import DataExporter

class DataExporterTest:
    def __init__(self, year, base_db_path='./industries/naics/duck_zipcode_db/zip_data/duck_db_manager/database', export_dir='./industires/US/zip'):
        self.year = year
        self.export_dir = export_dir
        self.base_db_path = base_db_path
        self.exporter = DataExporter(base_db_path=base_db_path, export_dir=export_dir, year=year)
        logging.basicConfig(level=logging.INFO)

    def _get_db_path_for_year(self, year):
        return os.path.join(self.base_db_path, f'us_naics_census_data_{year}.duckdb')

    def _count_rows_in_db(self, state, industry_level):
        if state is None:
            # Special handling for NotSpecified (None)
            query = f"""
            SELECT COUNT(*) AS row_count
            FROM DataEntry
            WHERE Year = {self.year}
            AND IndustryLevel = {industry_level}
            AND GeoID = '99999'
            """
            params = None
        else:
            query = f"""
            SELECT COUNT(*) AS row_count
            FROM DataEntry
            WHERE Year = {self.year}
            AND IndustryLevel = {industry_level}
            AND GeoID IN (SELECT GeoID FROM DimZipCode WHERE State = ?)
            """
            params = (state,)
        
        with duckdb.connect(self._get_db_path_for_year(self.year), read_only=True) as conn:
            try:
                if params:
                    result = conn.execute(query, params).fetchone()
                else:
                    result = conn.execute(query).fetchone()
                return result[0] if result else 0
            except Exception as e:
                logging.error(f"Failed to count rows in database for state {state}, industry level {industry_level}: {e}")
                return 0

    def _count_rows_in_csv(self, state, industry_level):
        if state is None:
            state = "NotSpecified"
            state_dir = os.path.join(self.export_dir, "NotSpecified")
        else:
            state_dir = os.path.join(self.export_dir, state)

        csv_file_path = os.path.join(state_dir, f'US-{state}-census-naics{industry_level}-zip-{self.year}.csv')
        
        if not os.path.exists(csv_file_path):
            logging.error(f"CSV file does not exist: {csv_file_path}")
            return 0

        try:
            df = pd.read_csv(csv_file_path)
            return len(df)
        except Exception as e:
            logging.error(f"Failed to read CSV file for state {state}, industry level {industry_level}: {e}")
            return 0

    def _compare_counts(self, state, industry_level):
        db_count = self._count_rows_in_db(state, industry_level)
        csv_count = self._count_rows_in_csv(state, industry_level)
        
        if db_count != csv_count:
            if state is None: state = "NotSpecified"
            logging.error(f"Row count mismatch for state {state}, industry level {industry_level}:")
            logging.error(f"Database count: {db_count}")
            logging.error(f"CSV count: {csv_count}")
            return False
        
        return True

    def run_test(self):
        levels = self.exporter.industry_levels
        states = self.exporter._fetch_states()

        all_tests_passed = True
        for state in states:
            for level in levels:
                if not self._compare_counts(state, level):
                    all_tests_passed = False
                    logging.error(f"Testing for Year {self.year}: Test failed for state {state}, industry level {level}")

        if all_tests_passed:
            print(f"Tested Year {self.year}: All CSV files match the database data by row count.")
            return True
        else:
            print(f"Tested Year {self.year}: Some CSV files do not match the database data by row count.")
            return False

# Example usage:
if __name__ == "__main__":
    tester = DataExporterTest(year=2019)
    tester.run_test()