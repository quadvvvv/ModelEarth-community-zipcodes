# Data Management Workflow

This project utilizes GitHub Actions to manage data related to community-zipcodes. The workflow automatically checks for updates, populates missing years, and exports data into CSV format.

## Folder Structure

```
.github/
└── scripts/
    ├── check_for_updates.py      # Script to check for missing years in the local database and fetch the latest year from the API.
    ├── export_data.py             # Script to export data for a specific year into CSV format.
    └── populate_data.py           # Script to populate the local database for missing years using the ZipPopulator class.
```

## GitHub Actions Workflow

The following GitHub Actions workflow is defined in `.github/workflows/data_management.yml`:

```yaml
name: Data Management Workflow

on:
  push:
    branches:
      - main
  workflow_dispatch:

jobs:
  data_management:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v2

    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.8'

    - name: Install dependencies
      run: |
        pip install -r requirements.txt

    - name: Check for updates
      id: check_updates  # Set an ID to reference this step's output
      run: |
        python .github/scripts/check_for_updates.py > gap_years.txt
        echo "::set-output name=gap_years::$(cat gap_years.txt)"  # Capture the output

    - name: Populate missing years
      run: |
        gap_years=${{ steps.check_updates.outputs.gap_years }}
        IFS=',' read -ra years <<< "$gap_years"  # Split the comma-separated string into an array
        for year in "${years[@]}"; do
          python .github/scripts/populate_data.py --year "$year"
        done

    - name: Export data
      run: |
        gap_years=${{ steps.check_updates.outputs.gap_years }}
        IFS=',' read -ra years <<< "$gap_years"  # Split the comma-separated string into an array
        for year in "${years[@]}"; do
          python .github/scripts/export_data.py --year "$year"  # Pass the year as an argument
        done
```

## Requirements

Ensure you have the following Python packages installed:

- `duckdb`
- `requests`
- `pandas`
- `tqdm`

You can install these dependencies by running:

```bash
pip install -r requirements.txt
```

## Usage

### Manual Trigger

The workflow can be manually triggered from the GitHub Actions tab in your repository. Alternatively, it runs automatically on push to the main branch.

### Scripts

To run individual scripts locally, you can use the following commands:

#### 1. Check for Updates

To check for missing years and populate the database, run:

```bash
python .github/scripts/check_for_updates.py
```

This script will:
- Fetch the latest available year from the U.S. Census API.
- Compare it with the existing local database to identify any missing years.
- If any gap years are found, it will call the population script to fill in the data.

#### 2. Export Data

To export data for a specific year, run:

```bash
python .github/scripts/export_data.py <year>
```

Replace `<year>` with the desired year, e.g., `2019`.

#### 3. Populate Data

To populate the database for missing years, this script is called by the `check_for_updates.py` script. You can run it directly if needed:

```bash
python .github/scripts/populate_data.py
```

## Contribution

Contributions are welcome! If you find any issues or have suggestions for improvements, please create an issue or submit a pull request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
