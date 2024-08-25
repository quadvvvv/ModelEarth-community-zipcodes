# ID Lists

This directory contains ID lists used in various data processing tasks, particularly for managing community-zipcode data. The primary file of interest is `industry_id_list.csv`, which plays a critical role in determining the industry IDs for data retrieval.

## Key Files

- **industry_id_list.csv**:  
  This file is generated in the notebook located at `industries/naics/us_econ-modify_test.ipynb`. It is essential for the following classes:

## Usage

### ZipPopulator Class
- **Method**: `get_zip_for_year`  
  This method utilizes the `industry_id_list.csv` to retrieve and process data relevant to specific ZIP codes based on the industry IDs defined within this file.

### DatabasePopulator Class
- **Method**: `populate_nacis`  
  This method uses `industry_id_list.csv` to populate the `DimNaics` table in the database, ensuring accurate representation of industry data.

## Overview
The files in this directory are crucial for maintaining the integrity and accuracy of the data processed in our applications. The `industry_id_list.csv` serves as a vital link between different data components, influencing the overall functionality of the data processing pipeline.

