import ibis.expr.types as it
import geopandas as gpd
from ..models import *
from tqdm import tqdm
import polars as pl
import pandas as pd
import requests
import logging
import ibis
import json
import os


class cleanData:
    def __init__(
        self,
        saving_dir: str = "data/",
        database_url: str = "duckdb:///data.ddb",
    ):
        self.database_url = database_url
        self.saving_dir = saving_dir
        self.data_file = self.database_url.split("///")[1]
        self.conn = ibis.duckdb.connect(f"{self.data_file}")

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename="data_process.log",
        )
        # Check if the saving directory exists
        if not os.path.exists(self.saving_dir + "raw"):
            os.makedirs(self.saving_dir + "raw")
        if not os.path.exists(self.saving_dir + "processed"):
            os.makedirs(self.saving_dir + "processed")
        if not os.path.exists(self.saving_dir + "external"):
            os.makedirs(self.saving_dir + "external")

        if not os.path.exists(self.saving_dir + "external/decode.json"):
            self.pull_file(
                url="https://github.com/ouslan/jp-QCEW/tree/main/data/external",
                filename=f"{self.saving_dir}external/decode.json",
            )

        if "qcewtable" not in self.conn.list_tables():
            init_qcew_table(self.data_file)
        if "hactable" not in self.conn.list_tables():
            init_hac_table(self.data_file)

    def make_qcew_dataset(self) -> None:
        """
        This function reads the raw data files in data/raw and inserts them into the database.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        for folder in os.listdir(f"{self.saving_dir}/raw"):
            count = 0
            if folder == ".gitkeep" or folder == ".DS_Store":
                continue
            else:
                for file in os.listdir(f"{self.saving_dir}/raw/{folder}"):
                    df = self.clean_txt(
                        f"{self.saving_dir}raw/{folder}/{file}",
                        f"{self.saving_dir}external/decode.json",
                    )
                    if df.empty:
                        logging.warning(f"File {file} is empty.")
                        continue
                    else:
                        self.conn.insert("qcewtable", df)
                        logging.info(
                            f"File {file} for {folder} has been inserted into the database."
                        )
                        count += 1

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
        column_names = [key for key in decode_file.keys()]
        widths = [value["length"] for value in decode_file.values()]
        slice_tuples = []
        offset = 0

        for i in widths:
            slice_tuples.append((offset, i))
            offset += i

        df = df.with_columns(
            [
                pl.col("full_str")
                .str.slice(slice_tuple[0], slice_tuple[1])
                .str.strip_chars()
                .alias(col)
                for slice_tuple, col in zip(slice_tuples, column_names)
            ]
        ).drop("full_str")
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
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:3454"
        )
        gdf = gdf.rename(columns={"geometry": "geom"})
        gdf = gdf.drop(columns=["longitude", "latitude"])
        gdf["geom"] = gdf["geom"].apply(lambda x: x.wkt)

        return gdf

    def group_by_naics_code(self) -> it.Table:
        """
        This function aggregate the data by year, quarter, and first 4 digits of the NAICS code.

        Parameters
        ----------
        None

        Returns
        -------
        it.Table
        """
        df = self.conn.table("qcewtable")

        df = df.mutate(
            total_employment=(
                df.first_month_employment
                + df.second_month_employment
                + df.third_month_employment
            )
            / 3
        )

        df = df.mutate(
            first_4_naics_code=df.naics_code.substr(0, 4),
            dummy=ibis.literal(1),
        )
        df = df.filter(df.first_4_naics_code != "")

        # Group by the specified columns and aggregate
        df = df.group_by(["year", "qtr", "first_4_naics_code"]).aggregate(
            total_wages_sum=df.total_wages.sum(),
            total_employment_sum=df.total_employment.sum(),
            dummy_sum=df.dummy.sum(),
        )

        df = df.filter(df["dummy_sum"] > 4)

        # Step 2: Add calculated columns for contributions
        df = df.mutate(
            fondo_contributions=df["total_wages_sum"] * 0.014,
            medicare_contributions=df["total_wages_sum"] * 0.0145,
            ssn_contributions=df["total_wages_sum"] * 0.062,
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
