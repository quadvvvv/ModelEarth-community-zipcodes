import os
import unittest
import pandas as pd
import duckdb
from duck_db_exporter import DataExporter

class TestDataExporterAccuracy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Set up a test database and insert controlled data for testing.
        """
        cls.test_db_path = 'test_database.duckdb'
        cls.test_export_dir = 'test_exports'
        cls.year = 2024
        
        # Create and populate the test database with controlled data
        with duckdb.connect(cls.test_db_path) as conn:
            conn.execute("""
                CREATE TABLE DimZipCode (
                    GeoID VARCHAR,
                    State VARCHAR
                )
            """)
            conn.execute("""
                INSERT INTO DimZipCode (GeoID, State) VALUES ('12345', 'CA'), ('67890', 'CA')
            """)
            conn.execute("""
                CREATE TABLE DataEntry (
                    GeoID VARCHAR,
                    NaicsCode VARCHAR,
                    Establishments INTEGER,
                    Employees INTEGER,
                    Payroll INTEGER,
                    Year INTEGER,
                    IndustryLevel INTEGER
                )
            """)
            conn.execute("""
                INSERT INTO DataEntry (GeoID, NaicsCode, Establishments, Employees, Payroll, Year, IndustryLevel)
                VALUES ('12345', '111', 10, 100, 5000, 2024, 2),
                       ('67890', '222', 20, 200, 10000, 2024, 2)
            """)
    
    def setUp(self):
        """
        Set up the DataExporter instance.
        """
        self.exporter = DataExporter(base_db_path=self.test_db_path, export_dir=self.test_export_dir, year=self.year)
    
    def tearDown(self):
        """
        Clean up test files and database.
        """
        if os.path.exists(self.test_export_dir):
            for root, dirs, files in os.walk(self.test_export_dir, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(self.test_export_dir)
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
    
    def test_make_csv_accuracy(self):
        """
        Test that the exported CSV files match the data in the test database.
        """
        self.exporter.make_csv('CA')
        
        # Load the exported CSV file
        file_path = os.path.join(self.test_export_dir, 'CA', f'US-CA-census-naics2-zip-{self.year}.csv')
        exported_df = pd.read_csv(file_path)
        
        # Query the test database for the expected data
        with duckdb.connect(self.test_db_path) as conn:
            expected_df = conn.execute("""
                SELECT GeoID AS Zipcode, NaicsCode, Establishments, Employees, Payroll
                FROM DataEntry
                WHERE Year = ?
                AND IndustryLevel = ?
                ORDER BY GeoID
            """, (self.year, 2)).fetchdf()
        
        # Compare the exported CSV with the expected data
        pd.testing.assert_frame_equal(exported_df, expected_df, check_like=True, check_dtype=True)

if __name__ == '__main__':
    unittest.main()
