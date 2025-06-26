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
        

    # def create_graph(self, naics_code : str) -> alt.Chart:

    #     naics_data = self.group_by_naics_code()
        
    #     filtered_df = naics_data.filter(naics_data['first_4_naics_code'] == naics_code)

    #     # Joining the Year and the quarter columns for graphing purpose
    #     filtered_df = filtered_df.mutate(
    #         year_qtr=(filtered_df['year'].cast("string") + ibis.literal("-Q") + filtered_df['qtr'].cast("string"))
    #         )
    #     filtered_pd = filtered_df.execute()
        
    #     filtered_pd = filtered_pd.sort_values(by=["year", "qtr"])

    #     chart = alt.Chart(filtered_pd).mark_line().encode(
    #         x=alt.X('year_qtr', title='Year', sort=list(filtered_pd['year_qtr'])),
    #         y=alt.Y('total_employment_sum:Q', title='Total Employment'),
    #         tooltip=[
    #             alt.Tooltip('year_qtr', title='Year and Quarter'),
    #             alt.Tooltip('total_employment_sum', title='Total Employment Sum')
    #             ]
    #         ).properties(
    #             title='Employment Trends for NAICS 5412',
    #             width=1000,
    #             height=400
    #             )
        
    #     return chart
    
    def gen_naics_graph(self, naics_code : str) -> alt.Chart:
        df_filtered, naics = self.get_naics_data(naics_code)

        chart = alt.Chart(df_filtered).mark_line().encode(
            x=alt.X('x_axis:N', title='Year'),
            y=alt.Y('total_employment_sum:Q', title='Total Employment'),
            tooltip=[
                    alt.Tooltip("x_axis", title="Time Period"),
                    alt.Tooltip("total_employment_sum", title="Total Employment")]
        ).properties(
            width='container',
        ).configure_view(
            fill='#e6f7ff'
        ).configure_axis(
            gridColor='white',
            grid=True
        )
        
        context = {
            'naics_code': naics,
        }
        return chart, context
    
    def gen_wages_graph(self, time_frame: str, naics_desc : str, data_type: str) -> alt.Chart:
        if data_type == 'nivel':
            column = 'taxable_wages'
        elif data_type == 'primera_diferencia':
            column = 'taxable_wages_diff'
        elif data_type == 'cambio_porcentual':
            column = 'taxable_wages_diff_p'
        df, naics = self.filter_wages_data(time_frame, naics_desc, column)

        x_values = df.select("time_period").unique().to_series().to_list()

        if time_frame == "quarterly":
            tick_vals = x_values[::3]
        else:
            tick_vals = x_values

        chart = alt.Chart(df).mark_line().encode(
            x=alt.X('time_period:N', title='Time Period', axis=alt.Axis(values=tick_vals)),
            y=alt.Y('nominas:Q', title='Nominas'),
            tooltip=[
                alt.Tooltip('time_period', title="Time Period"),
                alt.Tooltip('nominas', title="Wages")
                ]
        ).properties(
            width='container',
        ).configure_view(
            fill='#e6f7ff'
        ).configure_axis(
            gridColor='white',
            grid=True
        )

        return chart, naics
        
if __name__ == "__main__":
    g = graphGenerator()
    print(g.create_graph())
