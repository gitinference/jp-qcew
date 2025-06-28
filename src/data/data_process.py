import geopandas as gpd
from ..models import init_qcew_table, get_conn
from tqdm import tqdm
import polars as pl
import pandas as pd
import requests
import logging
import json
import os


class cleanData:
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.data_file = database_file
        self.conn = get_conn(self.data_file)

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
            self.pull_file(
                url="https://raw.githubusercontent.com/EconLabs/jp-QCEW/refs/heads/main/data/external/decode.json",
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
        None
        """
        if "qcewtable" not in self.conn.sql("SHOW TABLES;").df().get("name").tolist():
            init_qcew_table(self.data_file)
            for folder in os.listdir(f"{self.saving_dir}raw"):
                count = 0
                if folder == ".gitkeep" or folder == ".DS_Store":
                    continue
                else:
                    for file in os.listdir(f"{self.saving_dir}raw/{folder}"):
                        df = self.clean_txt(
                            f"{self.saving_dir}raw/{folder}/{file}",
                            f"{self.saving_dir}external/decode.json",
                        )
                        if df.empty:
                            logging.warning(f"File {file} is empty.")
                            continue
                        else:
                            self.conn.sql(
                                "INSERT INTO 'qcewtable' BY NAME SELECT * FROM df"
                            )
                            logging.info(
                                f"File {file} for {folder} has been inserted into the database."
                            )
                            count += 1
            return self.conn.sql("SELECT * FROM qcewtable").pl()
        else:
            return self.conn.sql("SELECT * FROM qcewtable").pl()

    def clean_txt(self, dev_file: str, decode_path: str) -> pd.DataFrame:
        """
        This function reads the raw txt files and cleans them up based on the decode file.

        Parameters
        ----------
        dev_file: str
            The path to the raw txt file.
        decode_path: str
            The path to the decode file.

        Returns
        -------
        pd.DataFrame
        """
        df = pl.read_csv(
            dev_file,
            separator="\n",
            has_header=False,
            encoding="latin1",
            new_columns=["full_str"],
        )

        decode_file = json.load(open(decode_path, "r"))
        column_names = list(decode_file.keys())

        # Create (start, length) tuples from decode_file using 0-based indexing
        slice_tuples = [
            (value["position"] - 1, value["length"]) for value in decode_file.values()
        ]

        # Use Polars to slice each field from the full string
        df = df.with_columns(
            [
                pl.col("full_str").str.slice(start, length).str.strip_chars().alias(col)
                for (start, length), col in zip(slice_tuples, column_names)
            ]
        ).drop("full_str")

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
        ).to_pandas()

        # Convert to GeoDataFrame
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:3454"
        )
        gdf = gdf.rename(columns={"geometry": "geom"}).drop(
            columns=["longitude", "latitude"]
        )
        gdf["geom"] = gdf["geom"].apply(lambda x: x.wkt)

        return gdf

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
        df = self.conn.sql("SELECT * FROM qcewtable").pl()

        df = df.with_columns(
            total_wages=(
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
        df = df.group_by(["year", "qtr", "first_4_naics_code"]).agg(
            total_wages_sum=pl.col("total_wages").sum(),
            total_employment_sum=pl.col("total_employment").mean(),
            dummy_sum=pl.col("dummy").sum(),
        )

        df = df.filter(pl.col("dummy_sum") > 4)

        # Step 2: Add calculated columns for contributions
        df = df.with_columns(
            fondo_contributions=pl.col("fondo_contributions") * 0.014,
            medicare_contributions=pl.col("medicare_contributions") * 0.0145,
            ssn_contributions=pl.col("ssn_contributions") * 0.062,
        )

        return df

    def unique_naics_code(self):
        df_qcew = self.group_by_naics_code()

        # Load the data
        df = self.conn.table("hactable")

        df = df.mutate(first_4_naics_code=df.NAICS_R02.substr(0, 4))

        df = df.mutate(first_4_naics_code=(df.first_4_naics_code.cast("int64")))
        df2 = df.join(df_qcew, predicates=("first_4_naics_code"))

        return df2
    
    def get_naics_data(self, naics_code: str) -> pl.DataFrame:
        current_path = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.abspath(os.path.join(current_path, "..", ".."))

        naics_data = pl.read_parquet(f"{base_dir}/{self.saving_dir}external/naics4_df.parquet")
        naics_desc_df = pl.read_excel(f"{self.saving_dir}raw/naics_codes.xlsx", sheet_id=1)
        invalid_naics_df = pl.read_excel(f"{self.saving_dir}raw/naics_codes.xlsx", sheet_id=2)

        invalid_codes = (
            invalid_naics_df
            .select(pl.col("naics_data").cast(pl.String))
            .to_series()
            .to_list()
        )

        naics_data = naics_data.join(
            naics_desc_df.select([
                pl.col("naics_code").cast(pl.String).alias("first_4_naics_code"),
                "naics_desc"
            ]),
            on="first_4_naics_code",
            how="left"
        )
        naics_data = naics_data.filter(pl.col("first_4_naics_code") != "0")
        naics_data = naics_data.filter(~pl.col("first_4_naics_code").is_in(invalid_codes))
    
        df_filtered = naics_data.filter(pl.col("naics_desc") == naics_code)
        df_filtered = df_filtered.filter(pl.col("year") < 2024)

        df_filtered = df_filtered.with_columns([
            pl.format("Q{} {}", pl.col("year"), pl.col("qtr"),).alias("x_axis")
        ])
        df_filtered = df_filtered.sort(["x_axis"], descending=False)

        naics = (
            naics_data
            .select("naics_desc")
            .unique()
            .sort(["naics_desc"], descending=False)
        ).to_series().to_list()

        return df_filtered, naics
    
    def get_wages_data(self, time_frame: str,) -> pl.DataFrame:
        naics_desc_df = pl.read_excel(f"{self.saving_dir}raw/naics_codes.xlsx", sheet_id=1)
        invalid_naics_df = pl.read_excel(f"{self.saving_dir}raw/naics_codes.xlsx", sheet_id=2)

        invalid_codes = (
            invalid_naics_df
            .select(pl.col("naics_data").cast(pl.String))
            .to_series()
            .to_list()
        )

        if time_frame == 'yearly':
            df = pl.read_csv(f"{self.saving_dir}raw/data_y.csv")
            df = df.with_columns((pl.col('year').cast(pl.Int32)).alias('time_period'))
        elif time_frame == 'fiscal':
            df = pl.read_csv(f"{self.saving_dir}raw/data_fy.csv")
            df = df.with_columns((pl.col('f_year').cast(pl.Int32)).alias('time_period'))
        elif time_frame == 'quarterly':
            df = pl.read_csv(f"{self.saving_dir}raw/data_q.csv")
            df = df.with_columns(
                (
                    pl.col("year").cast(pl.Int32).cast(pl.String)
                    + "-q"
                    + pl.col("qtr").cast(pl.Int32).cast(pl.String)
                ).alias('time_period')
            )
        else:
            raise ValueError("Invalid time frame.")
        
        df = df.with_columns(
            pl.col("naics_code").cast(pl.String).str.slice(0, 4).alias("naics_4digit")
        )

        df = df.join(
            naics_desc_df.select([
                pl.col("naics_code").cast(pl.String).alias("naics_4digit"),
                "naics_desc"
            ]),
            on="naics_4digit",
            how="left"
        )
        df = df.filter(pl.col("naics_4digit") != "0")
        df = df.filter(~pl.col("naics_4digit").is_in(invalid_codes))

        return df
    
    def filter_wages_data(self, time_frame: str, naics_desc: str, column: str):
        df = self. get_wages_data(time_frame)
        df = df.with_columns(
            pl.concat_str([
                pl.lit("(N"),
                pl.col("naics_4digit").cast(pl.Utf8),
                pl.lit(") "),
                pl.col("naics_desc")
            ]).alias("naics_desc")
        )
        df_filtered = df.filter(pl.col("naics_desc") == naics_desc)
        df_filtered = df_filtered.group_by(["time_period"]).agg([
            pl.col(column).cast(pl.Float64).sum().alias('nominas')
        ])
        df_filtered = df_filtered.sort(['time_period'])

        naics_desc = (
            df.select(pl.col("naics_desc"))
            .unique()
            .sort("naics_desc")
            .to_series()
            .to_list()
        )

        return df_filtered, naics_desc

    def pull_file(self, url: str, filename: str, verify: bool = True) -> None:
        """
        Pulls a file from a URL and saves it in the filename. Used by the class to pull external files.

        Parameters
        ----------
        url: str
            The URL to pull the file from.
        filename: str
            The filename to save the file to.
        verify: bool
            If True, verifies the SSL certificate. If False, does not verify the SSL certificate.

        Returns
        -------
        None
        """
        chunk_size = 10 * 1024 * 1024

        with requests.get(url, stream=True, verify=verify) as response:
            total_size = int(response.headers.get("content-length", 0))

            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Downloading",
            ) as bar:
                with open(filename, "wb") as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            file.write(chunk)
                            bar.update(
                                len(chunk)
                            )  # Update the progress bar with the size of the chunks
