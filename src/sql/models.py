import duckdb
from pathlib import Path


def get_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def init_qcew_table(db_path: str) -> None:
    current_dir = Path(__file__).parent
    sql_path = current_dir / "init_qcew_table.sql"
    with sql_path.open("r", encoding="utf-8") as file:
        qcew_script = file.read()

    conn = get_conn(db_path)
    conn.install_extension("spatial")
    conn.load_extension("spatial")
    conn.sql(qcew_script)


def init_hac_table(db_path: str) -> None:
    current_dir = Path(__file__).parent
    sql_path = current_dir / "init_hac_table.sql"
    with sql_path.open("r", encoding="utf-8") as file:
        hac_script = file.read()

    con = get_conn(db_path)
    con.sql(hac_script)
