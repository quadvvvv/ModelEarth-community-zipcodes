# DuckDB Zipcode Database Repository

This repository contains the `duck_zipcode_db` from the data pipeline. The goal is to create a standalone repository for all processing related to zipcode data.

## Completed Tasks
- Removed deprecated old SQL database codes.
- Updated documentations and annotations for python codes and notebooks under `duck_zipcode_db/`.
- Added new logic to generate yearly DuckDB files.
- Added new logic to export yearly DuckDB files.

## TODOs
1. (Optional) Check previous yearly databases to see if there any conlict and update correspondingly:
   1. Yearly Data Row Count Discrepancies. 
      1. Currently, checked up to 2020. (2017 and 2018 have discrepancies.)
   2. NAICS Level Discrepancies.
2. Add unit unit test for generate / export duckdb Files. 
3. Add a GitHub Action to automate the annual update of the database.
