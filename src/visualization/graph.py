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
    
    def gen_naics_graph(self, naics_code : str) -> alt.Chart:
        df_filtered, naics = self.get_naics_data(naics_code)

        line = alt.Chart(df_filtered).mark_line().encode(
            x=alt.X('x_axis:N', title='Year'),
            y=alt.Y('total_employment_sum:Q', title='Total Employment'),
            tooltip=['x_axis', 'total_employment_sum']
        )
        points = alt.Chart(df_filtered).mark_point(
            color='darkblue', 
            size=60,
            filled=True    
        ).encode(
            x='x_axis:N',
            y='total_employment_sum:Q',
            tooltip=['x_axis', 'total_employment_sum']
        )
        chart = (line + points).properties(
            title=f'Employment in the US by Quarter for NAICS {naics_code}',
            width=1000,
            height=200
        ).configure_view(
            fill='#e6f7ff'
        ).configure_axis(
            gridColor='white',
            grid=True
        ).configure_title(
            anchor='start',     
            fontSize=16,         
            color='#333333',      
            offset=30           
        )
        context = {
            'naics_code': naics,
        }
        return chart, context
        
if __name__ == "__main__":
    g = graphGenerator()
    print(g.create_graph())
