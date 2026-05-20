import importlib.resources as resources
import json
import logging
from pathlib import Path

import duckdb
import polars as pl


class CleanQCEW:
    def __init__(
        self,
        saving_dir: str = "data",
        log_file: str = "data_process.log",
    ):
        """
        Initializes the CleanQCEW pipeline, sets up directory structures,
        establishes an in-memory database connection, and configures logging.

        Parameters
        ----------
        saving_dir : str, default "data"
            The base directory path where raw, intermediate, and processed
            QCEW artifacts are stored or cached.
        log_file : str, default "data_process.log"
            The filename or path where pipeline execution logs will be written.

        Attributes
        ----------
        saving_dir : Path
            The pathlib.Path representation of the target saving directory.
        conn : duckdb.DuckDBPyConnection
            An isolated, in-memory DuckDB database session used for querying
            and aggregating data files.
        dict_file : str
            The absolute file path to the embedded `decode.json` schema layout,
            resolved dynamically from the package resources.
        """
        self.saving_dir = Path(saving_dir)
        self.conn = duckdb.connect()
        self.dict_file = str(resources.files("jp_qcew").joinpath("decode.json"))

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename=log_file,
        )

    def make_qcew_dataset(self) -> pl.DataFrame:
        """
        Processes raw QCEW text files, cleans and casts the data, saves them
        as Parquet files partitioned by year, and returns the aggregated dataset.

        This method iterates through the raw QCEW directories, skipping any data
        from the year 2002 or earlier. For valid files, it parses the schema,
        casts geospatial and economic columns to their proper data types, appends
        metadata attributes, and caches them to disk. Finally, it queries all
        processed Parquet files into a unified Polars DataFrame using the DuckDB
        driver.

        Parameters
        ----------
        None

        Returns
        -------
        pl.DataFrame
            A Polars DataFrame containing the combined historical QCEW dataset
            selected from all processed Parquet files.
        """

        qcew_dir = self.saving_dir / "qcew"

        for folder_path in qcew_dir.iterdir():
            if not folder_path.is_dir():
                continue

            count = 1
            year = str(folder_path)[10:14]
            if int(year) <= 2002:
                continue
            for file in folder_path.iterdir():
                year_dir = self.saving_dir / "processed" / "qcew" / str(year)
                year_dir.mkdir(parents=True, exist_ok=True)
                file_path = year_dir / f"data-{count}.parquet"

                if not file_path.exists():
                    df = self.clean_txt(str(file), self.dict_file)
                    df = df.with_columns(
                        pl.col("latitude").cast(pl.Float64, strict=False),
                        pl.col("longitude").cast(pl.Float64, strict=False),
                        pl.col("year").cast(pl.Int64, strict=False),
                        pl.col("qtr").cast(pl.Int64, strict=False),
                        pl.col("first_month_employment").cast(pl.Int64, strict=False),
                        pl.col("second_month_employment").cast(pl.Int64, strict=False),
                        pl.col("third_month_employment").cast(pl.Int64, strict=False),
                        pl.col("total_wages").cast(pl.Int64, strict=False),
                        pl.col("taxable_wages").cast(pl.Int64, strict=False),
                    )
                    df = df.with_columns(
                        file_year=pl.lit(year),
                        file_qtr=pl.lit(count),
                        year=pl.col("year").mode().first(),
                        qtr=pl.col("qtr").mode().first(),
                    )

                    df.write_parquet(file_path)
                    print(f"File {file} {count} has been inserted into the database.")
                    count += 1
                else:
                    count += 1

        search_path = self.saving_dir / "processed" / "qcew" / "**" / "data-*.parquet"

        return self.conn.execute(f"SELECT * FROM '{search_path}';").pl()

    def clean_txt(self, file_path: str, decode_path: str) -> pl.DataFrame:
        """
        Reads a fixed-width raw text file and parses it into a structured
        Polars DataFrame based on a JSON layout decoder.

        Parameters
        ----------
        file_path : str
            The path to the raw fixed-width text file.
        decode_path : str
            The path to the JSON decode file containing column names,
            starting positions, and field lengths.

        Returns
        -------
        pl.DataFrame
            A structured Polars DataFrame with columns stripped of surrounding
            whitespace and parsed according to the layout specifications.
        """

        with open(file_path, "r", encoding="latin1") as f:
            lines = [line.rstrip("\n") for line in f]

        # Create a Polars DataFrame with a single column: "raw_line"
        df = pl.DataFrame({"raw_line": lines})

        decode_file = json.load(open(decode_path, "r"))
        column_names = list(decode_file.keys())

        # Create (start, length) tuples from decode_file using 0-based indexing
        slice_tuples = [
            (value["position"] - 1, value["length"]) for value in decode_file.values()
        ]

        # Use Polars to slice each field from the full string
        df = df.with_columns(
            [
                pl.col("raw_line").str.slice(start, length).str.strip_chars().alias(col)
                for (start, length), col in zip(slice_tuples, column_names)
            ]
        ).drop("raw_line")

        return df

    def group_by_naics_code(self) -> pl.DataFrame:
        """
        This function aggregate the data by year, quarter, and first 4 digits of the NAICS code.

        Parameters
        ----------
        None

        Returns
        -------
        it.Table
        """
        df = self.conn.execute(f"""
            SELECT
                year,qtr,first_month_employment,second_month_employment,third_month_employment,naics_code,total_wages
                FROM '{self.saving_dir}processed/pr-qcew-*.parquet';
            """).pl()

        df = df.with_columns(
            total_employment=(
                pl.col("first_month_employment")
                + pl.col("second_month_employment")
                + pl.col("third_month_employment")
            )
            / 3
        )

        df = df.with_columns(
            naics4=pl.col("naics_code").str.slice(0, 4),
            dummy=pl.lit(1),
        )
        df = df.filter(pl.col("naics4") != "")

        # Group by the specified columns and aggregate
        df = df.group_by(["year", "qtr", "naics4"]).agg(
            total_wages=pl.col("total_wages").sum(),
            total_employment=pl.col("total_employment").mean(),
            dummy=pl.col("dummy").sum(),
        )

        df = df.filter(pl.col("dummy") > 4)

        # Step 2: Add calculated columns for contributions
        df = df.with_columns(
            fondo_contributions=pl.col("total_wages") * 0.014,
            medicare_contributions=pl.col("total_wages") * 0.0145,
            ssn_contributions=pl.col("total_wages") * 0.062,
        )

        return df

    def get_wages_data(
        self,
        time_frame: str,
    ) -> pl.DataFrame:
        naics_desc_df = pl.read_excel(
            f"{self.saving_dir}raw/naics_codes.xlsx", sheet_id=1
        )
        invalid_naics_df = pl.read_excel(
            f"{self.saving_dir}raw/naics_codes.xlsx", sheet_id=2
        )

        invalid_codes = (
            invalid_naics_df.select(pl.col("naics_data").cast(pl.String))
            .to_series()
            .to_list()
        )

        if time_frame == "yearly":
            df = pl.read_csv(f"{self.saving_dir}raw/data_y.csv")
            df = df.with_columns((pl.col("year").cast(pl.Int32)).alias("time_period"))
        elif time_frame == "fiscal":
            df = pl.read_csv(f"{self.saving_dir}raw/data_fy.csv")
            df = df.with_columns((pl.col("f_year").cast(pl.Int32)).alias("time_period"))
        elif time_frame == "quarterly":
            df = pl.read_csv(f"{self.saving_dir}raw/data_q.csv")
            df = df.with_columns(
                (
                    pl.col("year").cast(pl.Int32).cast(pl.String)
                    + "-q"
                    + pl.col("qtr").cast(pl.Int32).cast(pl.String)
                ).alias("time_period")
            )
        else:
            raise ValueError("Invalid time frame.")

        df = df.with_columns(
            pl.col("naics_code").cast(pl.String).str.slice(0, 4).alias("naics_4digit")
        )

        df = df.join(
            naics_desc_df.select(
                [
                    pl.col("naics_code").cast(pl.String).alias("naics_4digit"),
                    "naics_desc",
                ]
            ),
            on="naics_4digit",
            how="left",
        )
        df = df.filter(pl.col("naics_4digit") != "0")
        df = df.filter(~pl.col("naics_4digit").is_in(invalid_codes))

        return df

    def filter_wages_data(self, time_frame: str, naics_desc: str, column: str):
        df = self.get_wages_data(time_frame)
        df = df.with_columns(
            pl.concat_str(
                [
                    pl.lit("(N"),
                    pl.col("naics_4digit").cast(pl.Utf8),
                    pl.lit(") "),
                    pl.col("naics_desc"),
                ]
            ).alias("naics_desc")
        )
        df = df.filter(
            pl.col(column).is_not_null()
            & (pl.col(column).cast(pl.Utf8).str.strip_chars() != "")
        )
        df_filtered = df.filter(pl.col("naics_desc") == naics_desc)
        df_filtered = df_filtered.group_by(["time_period"]).agg(
            [pl.col(column).cast(pl.Float64).sum().alias("nominas")]
        )
        df_filtered = df_filtered.sort(["time_period"])

        naics_desc = (
            df.select(pl.col("naics_desc"))
            .unique()
            .sort("naics_desc")
            .to_series()
            .to_list()
        )

        return df_filtered, naics_desc
