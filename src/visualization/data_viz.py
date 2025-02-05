from ..data.data_process import cleanData


class dataViz(cleanData):
    def __init__(
        self, saving_dir: str = "data/", database_url: str = "duckdb:///data.ddb"
    ):
        super().__init__(saving_dir, database_url)

    def employment_graph(self):
        pass
