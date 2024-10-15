from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import json
import os
import polars as pl

class cleanData:
    def __init__(self, decode_path:str):
        self.decode_file = json.load(open(decode_path))
        self.data_dir = 'data/raw'

    def make_dataset(self):

        cleaned_df = pd.DataFrame(columns=self.decode_file.keys())
        for folder in os.listdir(self.data_dir):
            count = 0
            if folder == '.gitkeep':
                continue
            else:
                for file in os.listdir('data/raw/' + folder):
                    if os.path.exists('data/processed/' + file + '.csv'):
                        continue
                    else:
                        df = self.clean_txt('data/raw/' + folder + '/' + file)
                        cleaned_df = pd.concat([cleaned_df, df])
                        print('Processed '+ folder + '_' + str(count))
                        count += 1
        cleaned_df.to_csv('data/processed/cleaned_data.csv', index=False)

    def process_line(self, line):
        temp_df = []
        for key in self.decode_file:
            data = line[self.decode_file[key]['position']-1: self.decode_file[key]['position'] + self.decode_file[key]['length']-1]
            try:
                data = data.decode('utf-8')
            except UnicodeDecodeError:
                pass
            data = data.strip()
            temp_df.append(data)
        return temp_df

    def clean_txt(self, dev_file):
        df = pd.DataFrame(columns=self.decode_file.keys())
        with ThreadPoolExecutor() as executor:
            lines = [line for line in open(dev_file, 'rb')]
            results = list(executor.map(self.process_line, lines))
        df = pd.concat([df, pd.DataFrame(results, columns=self.decode_file.keys())])
        return df
    
    def group_by_naics_code(self):
        df = pl.read_csv("data/processed/master_df.csv", ignore_errors=True)
        
        # Create new column total_employment with the avg sum of (first_month_employment, second_month_employment and third_month_employment)
        df = df.with_columns(
        ((pl.col("first_month_employment") + pl.col("second_month_employment") + pl.col("third_month_employment")) / 3).alias("total_employment")
        )
        # Select the colums (total_wages, year, qtr, naics_code and total_employment) 
        df = df.select(pl.col("total_wages","year", "qtr", "naics_code", "total_employment"))
        # New column with the first 4 digits of the naics_code
        new_df_pd = df.with_columns(
        pl.col("naics_code").cast(pl.Utf8).str.slice(0,4).alias("first_4_naics_code"), 
        dummy=1
        )
        # Sum of the total_wages and total_employment to have the total_wages_sum column
        grouped_df = new_df_pd.group_by(["year", "qtr", "first_4_naics_code"]).agg((
            pl.col("total_wages") + pl.col("total_employment")).alias("total_wages_sum"), pl.col("dummy").sum().alias("dummy")
        )

        grouped_df = grouped_df.filter(pl.col("dummy") > 4)
        
        df.write_parquet("data/processed/master_df.parquet")
