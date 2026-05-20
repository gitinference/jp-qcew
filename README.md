# QCEW Data Processing Tool

> [!IMPORTANT]
> Project development has moved to [Codeberg](https://codeberg.org/gitinference/jp-qcew)

This tool is part of a collaboration between the University of Puerto Rico, Mayaguez, and Puerto Rico's Planning Board. Its main objective is to extract, clean, and process raw Quarterly Census of Employment and Wages (QCEW) data into structured formats optimized for high-performance economic and geographic analytics.

## Overview

The pipeline reads raw fixed-width QCEW data from the `data/qcew/` directory, processes it through an internal JSON layout schema layout, and caches intermediate tables into structured Parquet files partitioned by year.

The pipeline filters out records from the year 2002 or earlier, casts critical data metrics (employment indices, total/taxable wages), handles missing geospatial parameters safely, and leverages Polars and DuckDB to yield a single, integrated dataset for downstream workflows.

---

## Requirements

To run this tool, you will need the following Python packages:

- `duckdb`
- `polars`
- `geopandas`
- `pandas`
- `tqdm`
- `logging`

You can install the dependencies via `pip`:

```bash
pip install -r requirements.txt

```

Or utilize `uv` to lock and sync your environment instantly:

```bash
uv sync

```

---

## File Structure

The workspace expects files to be organized in the following directory layout:

```
data/
├── qcew/
│   ├── 2003/
│   │   ├── qcew_file_q1.txt
│   │   └── ...
│   ├── 2004/
│   └── ...
└── processed/
    └── qcew/
        ├── 2003/
        │   ├── data-1.parquet
        │   └── ...
        └── 2004/

```

- **`data/qcew/`**: Contains raw text data subfolders partitioned by year. **Note:** Folders with a year value $\le$ 2002 are automatically skipped by the processing architecture.
- **`data/processed/qcew/{year}/`**: Automatically generated storage location containing clean, structured data chunks saved as individual compressed `.parquet` files.

---

## How It Works

### 1. Initialization

The class (`CleanQCEW`) initializes tracking to your storage path, initializes an isolated in-memory `duckdb` connection session, configures runtime logging, and references the system's package-embedded `decode.json` structural layout via standard library resources.

### 2. Fast Fixed-Width Parsing

Raw textual inputs are streamed directly into Polars string blocks using multi-threaded null-byte delimiting, which cuts down overhead compared to standard Python line reading. Using the configuration from `decode.json`, fields are accurately sliced, cropped of padded spaces, and named.

### 3. Data Transformation & Alignment

- Columns representing geographical points (`latitude`, `longitude`), indices (`year`, `qtr`), and monetary metrics (`total_wages`, `taxable_wages`, monthly employment statistics) are cast to optimized types (`Float64` / `Int64`) safely without throwing schema exceptions.
- Metadata attributes (`file_year`, `file_qtr`) are appended natively before individual files are written out to target Parquet archives on disk.

### 4. Aggregation and Return

The tool uses an underlying DuckDB instance to query the full tree map of parquet files across all years in parallel, converting the aggregated database response directly into an in-memory `pl.DataFrame`.

---

## Key Functions

- **`__init__`**: Sets up pipeline directory mappings, spawns the central connection instance, and loads internal schema rules.
- **`make_qcew_dataset`**: Scans the input directories, runs the validation checks, manages chunk-saving states, and returns the final unified dataset.
- **`clean_txt`**: Performs raw text ingestion and extracts relevant structural fields based on layout specification boundaries.

---

## Usage

1. Organize your raw data folders inside your local storage folder (default: `data/qcew/`).
2. Run your pipeline orchestration module:

```bash
python main.py

```

---

## Logging

Operational timelines, warning updates, and operational status parameters are automatically streamed into a file called `data_process.log` formatted with active millisecond execution timestamps:

```text
20-May-26 07:45:12 - INFO - File data/qcew/2003/raw_data.txt 1 has been inserted into the database.

```

---

## License

This project is licensed under the [GNU General Public License v3.0](https://www.gnu.org/licenses/gpl-3.0.html). See the [LICENSE](https://www.google.com/search?q=LICENSE) file for more details.

## Contributing

Contributions to this tool are welcome. Please fork the repository and submit a pull request with any improvements or bug fixes on Codeberg.

---

## Cite

```bibtex
@software{ouslan2026jpqcew,
    author       = {Ouslan, Alejandro},
    title        = {JP-QCEW},
    month        = jan,
    year         = 2026,
    publisher    = {Zenodo},
    version      = {3.0.1},
    doi          = {10.5281/zenodo.18121581},
    url          = {https://doi.org/10.5281/zenodo.18121581}
}

```
