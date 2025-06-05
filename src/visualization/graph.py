from ..data.data_process import cleanData
import altair as alt
import pandas as pd
import ibis

class graphGenerator(cleanData):

    def _init_(
        self,
        saving_dir: str = "data/",
        database_url: str = "duckdb:///data.ddb"
        ):
        super().__init__(saving_dir,database_url)
        

    def create_graph(self, naics_code : str) -> alt.Chart:

        naics_data = self.group_by_naics_code()
        
        filtered_df = naics_data.filter(naics_data['first_4_naics_code'] == naics_code)

        # Joining the Year and the quarter columns for graphing purpose
        filtered_df = filtered_df.mutate(
            year_qtr=(filtered_df['year'].cast("string") + ibis.literal("-Q") + filtered_df['qtr'].cast("string"))
            )
        filtered_pd = filtered_df.execute()
        
        filtered_pd = filtered_pd.sort_values(by=["year", "qtr"])

        chart = alt.Chart(filtered_pd).mark_line().encode(
            x=alt.X('year_qtr', title='Year', sort=list(filtered_pd['year_qtr'])),
            y=alt.Y('total_employment_sum:Q', title='Total Employment'),
            tooltip=['year_qtr', 'total_employment_sum']
            ).properties(
                title='Employment Trends for NAICS 5412',
                width=1000,
                height=400
                )
        
        return chart
        
if __name__ == "__main__":
    g = graphGenerator()
    print(g.create_graph())
