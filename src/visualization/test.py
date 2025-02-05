import ibis
import polars as pl
import pandas as pd
from tqdm import tqdm
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

#con = ibis.duckdb.connect("data.ddb")
#df = con.table("qcewtable").execute().to_csv("data.csv")

df = pd.read_csv("test_data.csv", low_memory=False)
df = df.sort_values(by=["year", "qtr"])
dates = pd.date_range(start="2002q1", end="2024q3", freq="Q")
data = pd.DataFrame()
df_a = df.groupby(["year", "qtr"], as_index=False).sum().reset_index()
print(df_a)
monthly = pd.DataFrame()
j = 0
for i in tqdm(df_a.index):
    monthly.at[j, "year"] = df_a["year"].iloc[i]
    month = (df_a["qtr"].iloc[i] - 1) * 3
    monthly.at[j, "employment"] = df_a["first_month_employment"].iloc[i]
    monthly.at[j, "month"] = month + 1
    monthly.at[j+1, "year"] = df_a["year"].iloc[i]
    monthly.at[j+1, "employment"] = df_a["second_month_employment"].iloc[i]
    monthly.at[j+1, "month"] = month + 2
    monthly.at[j+2, "year"] = df_a["year"].iloc[i]
    monthly.at[j+2, "employment"] = df_a["third_month_employment"].iloc[i]
    monthly.at[j+2, "month"] = month + 3
    j = j + 3
print(monthly)