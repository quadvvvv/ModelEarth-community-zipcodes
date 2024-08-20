import sys
from industries.naics.duck_zipcode_db.exporter.data_exporter_validation_test import DataExporterTest

# Check if a year is provided as an argument
if len(sys.argv) != 2:
    print("Usage: python run_validation_test.py <year>")
    sys.exit(1)

year = int(sys.argv[1])  # Get the year from command-line arguments

# The default values for the Constructor's arguments are relative pahts, therefore, adjusted: 
base_db_path = '../industries/naics/duck_zipcode_db/zip_data/duck_db_manager/database/'
export_dir = '../industries/naics/US/zip'

tester = DataExporterTest(base_db_path=base_db_path, export_dir=export_dir, year=year)
tester.run_test()
