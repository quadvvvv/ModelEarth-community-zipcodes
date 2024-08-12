# DuckDB Zipcode Database Repository

This repository contains the `duck_zipcode_db` from the data pipeline. The goal is to create a standalone repository for all processing related to zipcode data.

## Completed Tasks
- Removed deprecated old SQL database codes.
- Updated documentations and annotations for python codes and notebooks under `duck_zipcode_db/`.
- Added new logic to generate yearly DuckDB files.

## TODOs
1. (Optional) Check previous yearly databases to see if there any conlict and update correspondingly:
   1. Yearly Data Row Count Discrepancies. 
   2. NAICS Level Discrepancies.
2. Create a script to save backups of the individual yearly DuckDB files in CSV format.
3. Develop a script to export cleaned/processed files to a specified location in the following format:
   - The output from Python should target NAICS levels `2`, `5`, and `6`, as shown below:
     ```
     community-zipcodes/industries/naics/US/zip/NY/US-NY-census-naics6-zip-2023.csv
     ```
4. Add a GitHub Action to automate the annual update of the database.
