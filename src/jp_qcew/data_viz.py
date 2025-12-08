import ibis
import pandas as pd
from tqdm import tqdm
import warnings
import altair as alt
from ..data.data_process import cleanData
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


class dataViz(cleanData):
    def __init__(
        self, saving_dir: str = "data/", database_url: str = "duckdb:///data.ddb"
        ):
        super().__init__(saving_dir, database_url)
        self.tables = ibis.duckdb.connect(database_url).execute()
        self.data = self.tables.table("qcewtable").execute()
        self.data_c = pd.concat([
            self.data["year"], 
            self.data["qtr"], 
            self.data["first_month_employment"], 
            self.data["second_month_employment"], 
            self.data["third_month_employment"]], 
            axis=1).sort_values(by=["year", "qtr"])
        self.data_m = self.to_monthly()

    def to_monthly(self):
        agg = self.data_c.groupby(["year", "qtr"], as_index=False).sum().reset_index()

        monthly = pd.DataFrame()
        j = 0

        for i in tqdm(agg.index):
            monthly.at[j, "year"] = agg["year"].iloc[i]
            monthly.at[j, "quarter"] = agg["qtr"].iloc[i]
            month = (agg["qtr"].iloc[i] - 1) * 3
            monthly.at[j, "employment"] = agg["first_month_employment"].iloc[i]
            monthly.at[j, "month"] = month + 1
            monthly.at[j+1, "year"] = agg["year"].iloc[i]
            monthly.at[j+1, "quarter"] = agg["qtr"].iloc[i]
            monthly.at[j+1, "employment"] = agg["second_month_employment"].iloc[i]
            monthly.at[j+1, "month"] = month + 2
            monthly.at[j+2, "year"] = agg["year"].iloc[i]
            monthly.at[j+2, "quarter"] = agg["qtr"].iloc[i]
            monthly.at[j+2, "employment"] = agg["third_month_employment"].iloc[i]
            monthly.at[j+2, "month"] = month + 3
            j = j + 3
        monthly["day"] = 1
        monthly["date"] = pd.to_datetime(monthly[["year", "month", "day"]])

        return monthly


    def to_yearly(self):
        yearly = pd.concat([self.data_m["year"], self.data_m["employment"]])
        yearly = self.data_m.groupby(["year"], as_index= False).mean().reset_index()
        yearly["month"] = 12
        yearly["day"] = 1
        yearly["date"] = pd.to_datetime(yearly[["year", "month", "day"]]).values.astype("datetime[Y]")
        
        return yearly


    def to_quarterly(self):
        quarterly = pd.concat([self.data_m["year"], self.data_m["quarter"], self.data_m["employment"]])
        quarterly = self.data_m.groupby(["year, quarter"], as_index= False).mean().reset_index()
        j = 0
        for i in quarterly["quarter"]:
            quarterly.at[j, "month"] = i * 3
            j = j + 1
        quarterly["day"] = 1
        quarterly["date"] = pd.to_datetime(quarterly[["year", "month", "day"]]).values.astype("datetime[Q]")
        
        return quarterly


    def get_timescale(self):
        scales = ["yearly", "quarterly", "monthly"]
        timescale = input("Select Timescale: ").casefold().strip()
        if timescale == scales[0]:
            to_visualize = self.to_yearly()
        elif timescale == scales[1]:
            to_visualize = self.to_quarterly()
        elif timescale == scales[2]:
            to_visualize = self.data_m
        
        return to_visualize


    def create_employment_graph(self):
        viz = self.get_timescale()
        graph = alt.Chart(viz).mark_line(interpolate="monotone").encode(
            x="date",
            y="employment"
        )
        return graph
