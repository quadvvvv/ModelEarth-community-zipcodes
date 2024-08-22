import os
import sys
import logging
from industries.naics.duck_zipcode_db.exporter.duck_db_exporter import DataExporter

# Set up logging
logging.basicConfig(level=logging.INFO)

def export_data_for_year(year):
    logging.info(f"Exporting data for year: {year}")
    try:
        exporter = DataExporter(
            base_db_path='./industries/naics/duck_zipcode_db/zip_data/duck_db_manager/database/',
            threads=4,
            export_dir='./industries/naics/US/zip',
            industry_levels=[2, 5, 6],  # Specify industry levels if needed
            year=year  # Specify the year for the data
        )
        exporter.make_csv()  # Run the export process
        logging.info("Data export completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during export: {e}")

if __name__ == "__main__":
    # Get the year from command line arguments
    if len(sys.argv) != 2:
        logging.error("Please provide a single year to export.")
        sys.exit(1)

    try:
        year_to_export = int(sys.argv[1])
        # Optionally, check if the year is within an expected range
        if year_to_export < 2012 or year_to_export > 2023:
            logging.error("Year must be between 2012 and 2023.")
            sys.exit(1)

        export_data_for_year(year_to_export)
    except ValueError:
        logging.error("Invalid year format. Please provide a numeric year.")
        sys.exit(1)
