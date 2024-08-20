import subprocess
import requests as r
import os
import datetime

class YearOutOfRangeException(Exception):
    """Exception raised when the current year is out of the supported range."""
    pass

def fetch_latest_year_from_api():
    """
    Fetch the latest year for which data exists from the API.

    Returns:
        int: The latest year for which data is available.

    Raises:
        YearOutOfRangeException: If the current year is less than 2012.
    """
    api_key = '975f39a54e48438ceebf303d6018e34db212e804'
    api_headers = {'x-api-key': api_key}  # Include API headers
    latest_year_remote = datetime.datetime.now().year  # Get the current year

    # Raise an exception if the current year is out of range
    if latest_year_remote < 2012:
        raise YearOutOfRangeException(f"Current year {latest_year_remote} is out of the supported range (2012 and above).")

    while not check_data_exists(latest_year_remote, api_headers):
        latest_year_remote -= 1  # Decrease the year if data doesn't exist

    return latest_year_remote

def check_data_exists(year, api_headers):
    """
    Check if data exists for a specified year.

    Args:
        year (int): The year to check.
        api_headers (dict): The headers to include in the API request.

    Returns:
        bool: True if data exists, False otherwise.
    """
    # Determine the correct endpoint based on the year
    if 2012 <= year <= 2018:
        url = f"https://api.census.gov/data/{year}/zbp"
    elif year > 2018:
        url = f"https://api.census.gov/data/{year}/cbp"
    else:
        print(f"Year {year} is out of the supported range.")
        return False

    try:
        response = r.get(url, headers=api_headers)

        if response.status_code in [200, 204]:
            return True  # Data exists for the specified year
        elif response.status_code == 404:
            return False  # No data for the specified year
        else:
            print(f"Unexpected status code {response.status_code} for year {year}")
            return False

    except r.exceptions.RequestException as e:
        print(f"Error while checking data for year {year}: {e}")
        return False

def check_local_years(database_path):
    """
    Check local database files to find the latest year.

    Args:
        database_path (str): The path to the directory containing database files.

    Returns:
        int: The latest year found in the local databases.
    """
    latest_year_local = 2012
    for filename in os.listdir(database_path):
        if filename.endswith(".duckdb"):
            # Extract the year from the filename
            year_str = filename.split('_')[-1].split('.')[0]  # Get the year part
            year = int(year_str)
            latest_year_local = max(latest_year_local, year)  # Update latest year found
    return latest_year_local

def find_gap_years(latest_year_local, latest_year_remote):
    return [year for year in range(latest_year_local + 1, latest_year_remote + 1)]

if __name__ == "__main__":
    db_directory = "../industries/naics/duck_zipcode_db/zip_data/duck_db_manager/database"
    try:
        latest_year = fetch_latest_year_from_api()
        local_years = check_local_years(db_directory)
        gap_years = find_gap_years(local_years, latest_year)

        # Output gap years as a comma-separated list
        print(','.join(map(str, gap_years)))  # Print gap years for GitHub Actions

    except YearOutOfRangeException as e:
        print(e)