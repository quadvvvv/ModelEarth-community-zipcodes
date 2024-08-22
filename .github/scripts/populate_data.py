import os
import sys
import logging
import requests as r
import industries.naics.duck_zipcode_db.populator.db_zip_populator as zp

# Set up logging
logging.basicConfig(level=logging.INFO)

def populate_data_for_year(year, api_headers):
    """Populate ZIP data for a given year using the ZipPopulator."""
    db_path = f"./industries/naics/duck_zipcode_db/zip_data/duck_db_manager/database/us_naics_census_data"

    # Create an instance of the ZipPopulator
    zip_populator = zp.ZipPopulator(industry_levels=[2, 5, 6], db_path=db_path, api_headers=api_headers, startyear=2012, endyear=year, separate_databases=True)

    # Call the method to get ZIP data for the specified year
    try:
        logging.info(f"Populating data for year: {year}")
        zip_populator.get_zip_for_year(year)
        logging.info(f"Data population for year {year} completed successfully.")
    except Exception as e:
        logging.error(f"Error populating data for year {year}: {e}")

if __name__ == "__main__":
    # Ensure the script is run with a year argument
    if len(sys.argv) != 2:
        print("Usage: python populate_data.py <year>")
        sys.exit(1)

    try:
        year = int(sys.argv[1])
        # Validate the year input
        if year < 2012:
            raise ValueError("Year must be 2012 or later.")

        # API headers to include with requests
        api_headers = {'x-api-key':'975f39a54e48438ceebf303d6018e34db212e804'}

        # Call the function to populate data for the specified year
        populate_data_for_year(year, api_headers)

    except ValueError as ve:
        logging.error(ve)
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        sys.exit(1)
