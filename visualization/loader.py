from pathlib import Path
import sqlite3
import pandas as pd

from visualization.config import PROJECT_ROOT


DATABASE_PATH = PROJECT_ROOT / "dane.db"


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DATABASE_PATH)


def load_results(
    algorithm: str | None = None,
    mode: str | None = None,
    dataset: str | None = None,
    data_size: int | None = None,
    cores: int | None = None,
    status: str | None = "OK",
) -> pd.DataFrame:
    query = """
        SELECT * FROM benchmark_results WHERE 1 = 1
    """

    parameters = []

    if status is not None:
        query += " AND status = ?"
        parameters.append(status)

    if algorithm is not None:
        query += " AND algorithm = ?"
        parameters.append(algorithm)

    if mode is not None:
        query += " AND mode = ?"
        parameters.append(mode)

    if dataset is not None:
        query += " AND dataset = ?"
        parameters.append(dataset)

    if data_size is not None:
        query += " AND data_size = ?"
        parameters.append(data_size)

    if cores is not None:
        query += " AND cores = ?"
        parameters.append(cores)

    query += """
        ORDER BY algorithm, dataset, data_size, cores
    """

    with get_connection() as connection:
        return pd.read_sql_query(
            query,
            connection,
            params=parameters,
        )


def load_all() -> pd.DataFrame:
    return load_results()


def get_distinct_values(column: str) -> list:
    query = f"""
        SELECT DISTINCT {column}
        FROM benchmark_results
        WHERE status = 'OK'
        ORDER BY {column}
    """

    with get_connection() as connection:
        df = pd.read_sql_query(query, connection,)

    return df[column].tolist()


def get_algorithms() -> list[str]:
    return get_distinct_values("algorithm")


def get_datasets() -> list[str]:
    return get_distinct_values("dataset")


def get_data_sizes() -> list[int]:
    return get_distinct_values("data_size")


def get_cores() -> list[int]:
    return get_distinct_values("cores")


def get_modes() -> list[str]:
    return get_distinct_values("mode")