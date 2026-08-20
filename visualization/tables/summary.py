import pandas as pd

from visualization.config import DEFAULT_DATASET, DEFAULT_DATA_SIZE, DEFAULT_CORES, SUMMARY_TABLE_DIR
from visualization.loader import load_all
from visualization.filters import filter_dataset, filter_data_size, filter_parallel, filter_sequential
from visualization.tables.common import export_table


def build_summary_table(
    df: pd.DataFrame,
    dataset: str = DEFAULT_DATASET,
    data_size: int = DEFAULT_DATA_SIZE,
    cores: int = DEFAULT_CORES,
) -> pd.DataFrame:

    filtered = filter_dataset(df, dataset)
    filtered = filter_data_size(filtered, data_size)

    sequential = filter_sequential(filtered).copy()
    parallel = filter_parallel(filtered).copy()
    parallel = parallel[parallel["cores"] == cores]

    if sequential.empty or parallel.empty:
        return pd.DataFrame()

    sequential = sequential[
        ["algorithm", "avg_time"]
    ].rename(
        columns={
            "avg_time": "Czas sekw. [s]",
        }
    )

    parallel = parallel[
        [
            "algorithm",
            "avg_time",
            "speedup",
            "efficiency",
        ]
    ].rename(
        columns={
            "avg_time": "Czas równ. [s]",
            "speedup": "Speedup",
            "efficiency": "Efficiency",
        }
    )

    result = sequential.merge(
        parallel,
        on="algorithm",
        how="inner",
    )

    result = result.rename(
        columns={
            "algorithm": "Algorytm",
        }
    )

    return result.sort_values("Czas równ. [s]").reset_index(drop=True)


def generate_summary_table() -> None:
    print()
    print(">>> Generowanie tabeli zbiorczej algorytmów...")

    df = load_all()

    table = build_summary_table(df)

    if table.empty:
        print("Brak danych do wygenerowania tabeli.")
        return

    export_table(
        df=table,
        directory=SUMMARY_TABLE_DIR,
        filename="summary_algorithms",
        caption=(
            "Porównanie badanych algorytmów dla zbioru random\\_int "
            "o rozmiarze 1~000~000 elementów i 8 jednostkach wykonawczych."
        ),
        label="tab:summary-algorithms",
        column_format="lrrrr",
    )