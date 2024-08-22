import os
import sys
import logging
from industries.naics.duck_zipcode_db.exporter.duck_db_exporter import DataExporter

# Set up logging for monitoring the export process
logging.basicConfig(level=logging.INFO)

def export_data_for_year(year):
    """
    Exports data for the specified year using the DataExporter.

    Args:
        year (int): The year for which data should be exported.

    Raises:
        Exception: If any error occurs during the export process.
    """
    logging.info(f"Exporting data for year: {year}")
    try:
        # Initialize the DataExporter with necessary parameters
        exporter = DataExporter(
            base_db_path='./industries/naics/duck_zipcode_db/zip_data/duck_db_manager/database/',
            threads=4,  # Number of threads to use for export
            export_dir='./industries/naics/US/zip',  # Directory to save exported CSV files
            industry_levels=[2, 5, 6],  # Specify industry levels if needed
            year=year  # Year for the data export
        )
        exporter.make_csv()  # Run the export process
        logging.info("Data export completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during export: {e}")

if __name__ == "__main__":
    # Main execution block
    # Get the year from command line arguments
    if len(sys.argv) != 2:
        logging.error("Please provide a single year to export.")
        sys.exit(1)

    try:
        year_to_export = int(sys.argv[1])
        # Check if the year is within an expected range
        if year_to_export < 2012:
            logging.error("Year must be 2012 or later.")
            sys.exit(1)

        # Call the function to export data for the specified year
        export_data_for_year(year_to_export)
    except ValueError:
        logging.error("Invalid year format. Please provide a numeric year.")
        sys.exit(1)
