# DuckDB Zipcode Database Repository

This repository contains the `duck_zipcode_db` from the data pipeline. The goal is to create a standalone repository for all processing related to zipcode data.

## Completed Tasks
- Removed deprecated old SQL database codes.

## TODOs
1. Add a script to generate individual yearly DuckDB files.
2. Add a script to save backups for the individual yearly DuckDB files in CSV format.
3. Add a script to export the cleaned/processed files to a desired location with the following format:
   - From Python, the output would be sent to `naics 2`, `5`, and `6` as follows:
     ```
     community-zipcodes/industries/naics/US/zip/NY/US-NY-census-naics6-zip-2023.csv
     ```
