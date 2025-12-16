import duckdb
import polars as pl
import logging
import jp_tools
import json
import os


class CleanQCEW:
    def __init__(
        self,
        saving_dir: str,
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.data_file = database_file
        self.conn = duckdb.connect()

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename=log_file,
        )
        # Check if the saving directory exists
        if not os.path.exists(self.saving_dir + "raw"):
            os.makedirs(self.saving_dir + "raw")
        if not os.path.exists(self.saving_dir + "processed"):
            os.makedirs(self.saving_dir + "processed")
        if not os.path.exists(self.saving_dir + "external"):
            os.makedirs(self.saving_dir + "external")
        if not os.path.exists(f"{self.saving_dir}external/decode.json"):
            jp_tools.download(
                url="https://raw.githubusercontent.com/gitinference/jp-QCEW/refs/heads/main/data/external/decode.json",
                filename=f"{self.saving_dir}external/decode.json",
            )

    def make_qcew_dataset(self) -> pl.DataFrame:
        """
        This function reads the raw data files in data/raw and inserts them into the database.

        Parameters
        ----------
        None

        Returns
        -------
        Returns a polars DataFrame containing all the inserted data
        """
        for folder in os.listdir(f"{self.saving_dir}qcew"):
            count = 0
            if folder == ".gitkeep" or folder == ".DS_Store":
                continue
            else:
                for file in os.listdir(f"{self.saving_dir}qcew/{folder}"):
                    if os.path.exists(
                        f"{self.saving_dir}processed/pr-qcew-{folder}-{count}"
                    ):
                        continue
                    df = self.clean_txt(
                        f"{self.saving_dir}qcew/{folder}/{file}",
                        f"{self.saving_dir}external/decode.json",
                    )
                    if df.is_empty():
                        logging.warning(f"File {file} is empty.")
                        continue
                    else:
                        year = df.select(pl.col("year").mode()).item()
                        qtr = df.select(pl.col("qtr").mode()).item()
                        df.write_parquet(
                            file=f"{self.saving_dir}processed/pr-qcew-{year}-{qtr}.parquet"
                        )
                        logging.info(
                            f"File {file} for {folder} has been inserted into the database."
                        )
                        count += 1
        return self.conn.execute(
            f"SELECT * FROM '{self.saving_dir}processed/pr-qcew-*.parquet';"
        ).pl()

    def clean_txt(self, file_path: str, decode_path: str) -> pl.DataFrame:
        """
        This function reads the raw txt files and cleans them up based on the decode file.

        Parameters
        ----------
        file_path: str
            The path to the raw txt file.
        decode_path: str
            The path to the decode file.

        Returns
        -------
        pd.DataFrame
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

        # Cast numeric fields
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
        df = self.conn.execute(
            f"""
            SELECT
                year,qtr,first_month_employment,second_month_employment,third_month_employment,naics_code,total_wages
                FROM '{self.saving_dir}processed/pr-qcew-*.parquet';
            """
        ).pl()

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
