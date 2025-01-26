# QCEW Data Processing Tool

This tool is part of a collaboration between the University of Puerto Rico, Mayaguez, and Puerto Rico's Planning Board. Its main objective is to convert raw QCEW data into a DuckDB database with a geographic (geom) column that stores the coordinates of businesses.

## Overview

The script takes raw Quarterly Census Employment and Wages (QCEW) data from the `data/raw` directory, processes it, and stores it in a DuckDB database. The raw data should be organized in subfolders by year (e.g., `data/raw/2002`, `data/raw/2003`), with each year folder containing the data for each quarter. The resulting database will be available in the `data` directory with a `.ddb` extension.

This tool also incorporates geospatial data, using latitude and longitude values to create a point geometry (`geom`) for each business, allowing for geographic queries.

## Requirements

To run this tool, you will need the following Python packages:

- `ibis`
- `duckdb`
- `polars`
- `geopandas`
- `tqdm`
- `requests`
- `json`
- `logging`
- `pandas`

You can install the necessary dependencies using:

```bash
pip install -r requirements.txt
```

Or utilize the `uv` to ensure compatibility

```
uv sync
```

## File Structure

The data should be organized in the following structure:

```
data/
├── raw/
│   ├── 2002/
│   ├── 2003/
│   └── ...
├── processed/
├── external/
│   └── decode.json
└── data.ddb
```

- **`data/raw/`**: This directory contains the raw QCEW data, organized by year and quarter.
- **`data/processed/`**: This directory is for storing processed data.
- **`data/external/`**: This directory contains external files, including `decode.json`, which is required for decoding the raw data files.
- **`data.ddb`**: The output DuckDB database containing the processed data.

## How It Works

1. **Initialization**: The script checks for necessary directories (raw, processed, external) and creates them if they don't exist. It also downloads external files, such as `decode.json`, if not already present. This file holds the Census codification of the data.

2. **Data Processing**:
   - The tool reads raw data files, cleans them, and extracts relevant fields based on predefined column widths defined in `decode.json`.
   - The cleaned data includes geographic coordinates (latitude and longitude), which are then transformed into a `geom` column of type `Point`.
   - This processed data is inserted into a DuckDB database.

3. **Group and Aggregate Data**:
   - The data is grouped by `NAICS` code (4-digit), year, and quarter, aggregating information such as total wages and total employment.
   - Additional calculations are performed for contributions to the social security, Medicare, and other funds.

4. **Joining with External Data**:
   - The tool also allows for joining the QCEW data with external data (e.g., `hactable`) based on `NAICS` codes, facilitating further analysis.

## Key Functions

- **`make_qcew_dataset`**: Processes all the raw QCEW data and inserts it into the DuckDB database.
- **`clean_txt`**: Cleans and formats the raw text data, extracting relevant fields and generating geographic information.
- **`group_by_naics_code`**: Groups data by `NAICS` code and aggregates the total wages and employment.
- **`unique_naics_code`**: Joins the grouped QCEW data with external data based on the `NAICS` code.
- **`pull_file`**: Downloads external files from a given URL (e.g., `decode.json`).

## Usage

1. Organize your raw QCEW data by year and quarter in the `data/raw/` folder.
2. Ensure that `decode.json` is in the `data/external/` folder.
3. Run the script to process the data:

```bash
python main.py
```

## Logging

The script logs key events and warnings to a file called `data_process.log`. This includes information about successfully processed files, warnings for empty files, and other runtime details.

## License

This project is licensed under the [GNU General Public License v3.0](https://www.gnu.org/licenses/gpl-3.0.html). See the [LICENSE](LICENSE) file for more details.

## Contributing

Contributions to this tool are welcome. Please fork the repository and submit a pull request with any improvements or bug fixes.

If you have any questions or need further assistance, feel free to reach out!
