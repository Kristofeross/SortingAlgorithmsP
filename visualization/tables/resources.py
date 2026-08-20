import pandas as pd

from visualization.config import DEFAULT_DATASET, DEFAULT_DATA_SIZE, DEFAULT_CORES, RESOURCES_TABLE_DIR
from visualization.loader import load_all
from visualization.filters import filter_dataset, filter_data_size, filter_parallel
from visualization.tables.common import export_table


def build_resources_table(
    df: pd.DataFrame,
    dataset: str = DEFAULT_DATASET,
    data_size: int = DEFAULT_DATA_SIZE,
    cores: int = DEFAULT_CORES,
) -> pd.DataFrame:

    filtered = filter_dataset(df, dataset)
    filtered = filter_data_size(filtered, data_size)
    filtered = filter_parallel(filtered)
    filtered = filtered[filtered["cores"] == cores]

    if filtered.empty:
        return pd.DataFrame()

    result = filtered[
        [
            "algorithm",
            "avg_cpu",
            "avg_mem",
            "max_mem",
        ]
    ].copy()

    result = result.rename(
        columns={
            "algorithm": "Algorytm",
            "avg_cpu": "CPU [%]",
            "avg_mem": "RAM avg [MB]",
            "max_mem": "RAM max [MB]",
        }
    )

    return result.sort_values("Algorytm").reset_index(drop=True)


def generate_resources_table() -> None:
    print()
    print(">>> Generowanie tabeli wykorzystania zasobów...")

    df = load_all()

    table = build_resources_table(df)

    if table.empty:
        print("Brak danych do tabeli zasobów.")
        return

    export_table(
        df=table,
        directory=RESOURCES_TABLE_DIR,
        filename="resources",
        caption=(
            "Średnie obciążenie CPU oraz wykorzystanie pamięci RAM "
            "dla zbioru random\\_int o rozmiarze 1~000~000 elementów "
            "i 8 jednostkach wykonawczych."
        ),
        label="tab:resources",
        column_format="lrrr",
    )