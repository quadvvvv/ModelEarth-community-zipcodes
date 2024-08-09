# DuckDB Zipcode Database Repository

This repository contains the `duck_zipcode_db` from the data pipeline. The goal is to create a standalone repository for all processing related to zipcode data.

## Completed Tasks
- Removed deprecated old SQL database codes.
- Updated documentations and annotations for python codes and notebooks under `duck_zipcode_db/`.

## TODOs
1. Implement a script to generate individual yearly DuckDB files.
   - Currently, the Python code generates NAICS levels using hardcoded values.
   - The updated version should accept parameters and update the database accordingly.
   - Try to modify the `get_zip_for_year(self, year)` method in the ZipPopulator class to accommodate the new parameterized implementation.
2. Create a script to save backups of the individual yearly DuckDB files in CSV format.
3. Develop a script to export cleaned/processed files to a specified location in the following format:
   - The output from Python should target NAICS levels `2`, `5`, and `6`, as shown below:
     ```
     community-zipcodes/industries/naics/US/zip/NY/US-NY-census-naics6-zip-2023.csv
     ```
4. Add a GitHub Action to automate the annual update of the database.
