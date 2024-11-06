from ..dao.qcew_raw import create_qcew
from sqlmodel import create_engine
import polars as pl
import ibis
import json
import os

class cleanData:
    def __init__(self, decode_path:str, database_url:str="sqlite:///db.sqlite", debug:bool=True):
        self.decode_file = json.load(open(decode_path))
        self.database_url = database_url
        self.engine = create_engine(self.database_url)
        self.data_dir = 'data/raw'
        self.debug = debug

        if self.database_url.startswith("sqlite"):
            self.conn = ibis.sqlite.connect(self.database_url.replace("sqlite:///", ""))
        elif self.database_url.startswith("postgres"):
            self.conn = ibis.postgres.connect(
                user=self.database_url.split("://")[1].split(":")[0],
                password=self.database_url.split("://")[1].split(":")[1].split("@")[0],
                host=self.database_url.split("://")[1].split(":")[1].split("@")[1],
                port=self.database_url.split("://")[1].split(":")[2].split("/")[0],
                database=self.database_url.split("://")[1].split(":")[2].split("/")[1])
        else:
            raise Exception("Database url is not supported")
        if "qcewtable" not in self.conn.list_tables():
            create_qcew(self.engine)

    def make_dataset(self):

        for folder in os.listdir(self.data_dir):
            count = 0
            if folder == '.gitkeep':
                continue
            else:
                for file in os.listdir('data/raw/' + folder):
                    if os.path.exists('data/processed/' + file + '.parquet'):
                        continue
                    else:
                        self.clean_txt(f"data/raw/{folder}/{file}")
                        count += 1

    def clean_txt(self, dev_file:str) -> None:
        df = pl.read_csv(
            dev_file,
            separator="\n",
            has_header=False,
            encoding="latin1",
            new_columns=["full_str"]
        )
        column_names = [key for key in self.decode_file.keys()]
        widths = [value["length"] for value in self.decode_file.values()]
        slice_tuples = []
        offset = 0

        for i in widths:
            slice_tuples.append((offset, i))
            offset += i

        df = df.with_columns(
            [
            pl.col("full_str").str.slice(slice_tuple[0], slice_tuple[1]).str.strip_chars().alias(col)
            for slice_tuple, col in zip(slice_tuples, column_names)
            ]).drop("full_str")

        self.conn.insert("qcewtable", df.cast(pl.String))
        self.debug_log(f"Inserted {dev_file} into the database")

    def group_by_naics_code(self):
        df = pl.read_csv("data/processed/cleaned_data.csv", ignore_errors=True)

        # Create new column total_employment with the avg sum of (first_month_employment, second_month_employment and third_month_employment)
        df = df.with_columns(
        ((pl.col("first_month_employment") + pl.col("second_month_employment") + pl.col("third_month_employment")) / 3).alias("total_employment")
        )
        # Select the colums (total_wages, year, qtr, naics_code and total_employment) 
        df = df.select(pl.col("total_wages","year", "qtr", "naics_code", "total_employment", "taxable_wages", "contributions_due"))
        # New column with the first 4 digits of the naics_code
        new_df_pd = df.with_columns(
        pl.col("naics_code").cast(pl.Utf8).str.slice(0,4).alias("first_4_naics_code"),
        dummy=1
        )
        # Sum of the total_wages and total_employment to have the total_wages_sum column
        grouped_df = new_df_pd.group_by(["year", "qtr", "first_4_naics_code"]).agg(
            pl.col("total_wages").sum().alias("total_wages_sum"),
            pl.col("total_employment").sum().alias("total_employment_sum"),
            pl.col("dummy").sum().alias("dummy")
        )

        grouped_df = grouped_df.filter(pl.col("dummy") > 4)
        return df

    def unique_naics_code(self):
        df1 = self.group_by_naics_code()
        unique_df = pl.read_excel("data/processed/Query1_hac.xlsx")
        unique_df = unique_df.with_columns(
        (pl.col("NAICS_R02").cast(pl.Utf8).str.slice(0,4).alias("first_4_naics_code"))
        )
        unique_df = unique_df.unique(subset=["first_4_naics_code"])
        unique_df = unique_df.sort("SEM_NUM_PAT")

        print(unique_df)

        final_df = df1.join(unique_df, on="first_4_naics_code", how="inner", validate="m:1")

        print("Final data frame")

        final_df = final_df.sort("year")

    def debug_log(self, message:str) -> None:
        if self.debug:
            print(f"\033[0;36mINFO: \033[0m {message}")
