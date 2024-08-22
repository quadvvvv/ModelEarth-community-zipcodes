import os
import sys
import logging
import requests as r
import industries.naics.duck_zipcode_db.populator.db_zip_populator as zp

# Set up logging for monitoring the population process
logging.basicConfig(level=logging.INFO)

def populate_data_for_year(year, api_headers):
    """
    Populate ZIP data for a given year using the ZipPopulator.

    Args:
        year (int): The year for which ZIP data should be populated.
        api_headers (dict): Headers to include with the API requests.

    Raises:
        Exception: If any error occurs during the population process.
    """
    db_path = "./industries/naics/duck_zipcode_db/zip_data/duck_db_manager/database/us_naics_census_data"

    # Create an instance of the ZipPopulator
    zip_populator = zp.ZipPopulator(
        industry_levels=[2, 5, 6],  # Specify industry levels if needed
        db_path=db_path,  # Database path for ZIP data
        api_headers=api_headers,  # API headers for requests
        startyear=2012,  # Starting year for data population
        endyear=year,  # End year for data population
        separate_databases=True  # Option to create separate databases for each year
    )

    # Call the method to get ZIP data for the specified year
    try:
        logging.info(f"Populating data for year: {year}")
        zip_populator.get_zip_for_year(year)  # Execute data population
        logging.info(f"Data population for year {year} completed successfully.")
    except Exception as e:
        logging.error(f"Error populating data for year {year}: {e}")

if __name__ == "__main__":
    # Main execution block
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
        api_headers = {'x-api-key': '975f39a54e48438ceebf303d6018e34db212e804'}

        # Call the function to populate data for the specified year
        populate_data_for_year(year, api_headers)

    except ValueError as ve:
        logging.error(ve)
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        sys.exit(1)
