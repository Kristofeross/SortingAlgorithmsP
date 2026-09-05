import pandas as pd

from visualization.config import DEFAULT_DATASET, DEFAULT_DATA_SIZE, DEFAULT_CORES, SUMMARY_TABLE_DIR
from visualization.loader import load_all
from visualization.filters import filter_dataset, filter_data_size, filter_parallel, filter_sequential
from visualization.tables.common import export_table, format_mean_std


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

    sequential["Czas sekw. [s]"] = sequential.apply(
        lambda row: format_mean_std(
            row["avg_time"],
            row["std_time"],
        ),
        axis=1,
    )

    sequential = sequential[
        [
            "algorithm",
            "Czas sekw. [s]",
        ]
    ]

    parallel["Czas równ. [s]"] = parallel.apply(
        lambda row: format_mean_std(
            row["avg_time"],
            row["std_time"],
        ),
        axis=1,
    )

    parallel["sort_time"] = parallel["avg_time"]

    parallel = parallel[
        [
            "algorithm",
            "Czas równ. [s]",
            "speedup",
            "efficiency",
            "sort_time",
        ]
    ].rename(
        columns={
            "speedup": "Przyspieszenie",
            "efficiency": "Efektywność",
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

    result = result.sort_values(
        "sort_time"
    ).reset_index(drop=True)

    result = result.drop(
        columns=["sort_time"]
    )

    return result

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
            "Porównanie badanych algorytmów dla zbioru "
            "random\\_int o rozmiarze 1~000~000 elementów "
            "i 8 jednostkach wykonawczych. Czasy przedstawiono jako "
            "średnią wraz z odchyleniem standardowym."
        ),
        label="tab:summary-algorithms",
        column_format="lrrrr",
    )