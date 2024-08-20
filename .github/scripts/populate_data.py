import os
import sys
import requests as r
from industries.naics.duck_zipcode_db.populator import db_zip_populator as zp

def populate_data_for_year(year, api_headers):
    db_path = f"../industries/naics/duck_zipcode_db/zip_data/duck_db_manager/database/us_naics_census_data_{year}.duckdb"

    # Create an instance of the ZipPopulator
    zip_populator = zp.ZipPopulator(industry_levels=[2, 5, 6], db_path=db_path, api_headers=api_headers, startyear=2012, endyear=year, separate_databases=True)

    # Call the method to get ZIP data for the specified year
    zip_populator.get_zip_for_year(year)

if __name__ == "__main__":
    # Ensure the script is run with a year argument
    if len(sys.argv) != 2:
        print("Usage: python populate_data.py <year>")
        sys.exit(1)

    year = int(sys.argv[1])

    # API headers to include with requests
    api_key = '975f39a54e48438ceebf303d6018e34db212e804'
    api_headers = {'x-api-key': api_key}

    # Call the function to populate data for the specified year
    populate_data_for_year(year, api_headers)
