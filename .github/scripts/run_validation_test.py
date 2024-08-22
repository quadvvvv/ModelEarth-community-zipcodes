import sys
from industries.naics.duck_zipcode_db.exporter.data_exporter_validation_test import DataExporterTest

# Check if a year is provided as an argument
if len(sys.argv) != 2:
    print("Usage: python run_validation_test.py <year>")
    sys.exit(1)

# Get the year from command-line arguments
year = int(sys.argv[1])

# Define the base database path and export directory
# These paths are relative to the root of the repository
base_db_path = './industries/naics/duck_zipcode_db/zip_data/duck_db_manager/database/'
export_dir = './industries/naics/US/zip'

# Create an instance of the DataExporterTest
tester = DataExporterTest(base_db_path=base_db_path, export_dir=export_dir, year=year)

# Run the validation test
result = tester.run_test()

# Check the result of the validation test
if result:
    print(f"Validation succeeded for year {year}.")
    sys.exit(0)  # Exit with 0 for success
else:
    print(f"Validation failed for year {year}.")
    sys.exit(1)  # Exit with 1 for failure
