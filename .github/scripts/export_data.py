import os
import sys
from industries.naics.duck_zipcode_db.populator.db_zip_populator import DataExporter

def export_data_for_year(year):
    print(f"Exporting data for year: {year}")
    exporter = DataExporter(
        base_db_path='../industries/naics/duck_zipcode_db/zip_data/duck_db_manager/database/',
        threads=4,
        export_dir='../industries/naics/US/zip',
        industry_levels=[2, 5, 6],  # Specify industry levels if needed
        year=year  # Specify the year for the data
    )
    exporter.make_csv()  # Run the export process

if __name__ == "__main__":
    # Get the year from command line arguments
    if len(sys.argv) != 2:
        print("Please provide a single year to export.")
        sys.exit(1)

    year_to_export = int(sys.argv[1])
    
    export_data_for_year(year_to_export)
