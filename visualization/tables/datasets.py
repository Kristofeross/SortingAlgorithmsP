import pandas as pd

from visualization.config import DEFAULT_DATA_SIZE, DEFAULT_CORES, DATASETS_TABLE_DIR, DATASET_LABELS
from visualization.loader import load_all
from visualization.filters import filter_data_size, filter_parallel
from visualization.tables.common import export_table


def build_dataset_impact_table(
    df: pd.DataFrame,
    data_size: int = DEFAULT_DATA_SIZE,
    cores: int = DEFAULT_CORES,
) -> pd.DataFrame:

    filtered = filter_data_size(df, data_size)
    filtered = filter_parallel(filtered)
    filtered = filtered[
        filtered["cores"] == cores
    ].copy()

    if filtered.empty:
        return pd.DataFrame()

    filtered["dataset_label"] = filtered["dataset"].map(
        lambda value: DATASET_LABELS.get(value, value)
    )

    pivot = filtered.pivot_table(
        index="dataset_label",
        columns="algorithm",
        values="avg_time",
        aggfunc="mean",
    )

    pivot = pivot.reset_index()

    pivot = pivot.rename(
        columns={
            "dataset_label": "Zbiór danych",
        }
    )

    preferred_order = [
        "Zbiór danych",
        "Quick Sort",
        "Merge Sort",
        "Bucket Sort",
        "Sample Sort",
    ]

    available = [
        column
        for column in preferred_order
        if column in pivot.columns
    ]

    return pivot[available]


def generate_dataset_impact_table() -> None:
    print()
    print(">>> Generowanie tabeli wpływu danych wejściowych...")

    df = load_all()

    table = build_dataset_impact_table(df)

    if table.empty:
        print("Brak danych do tabeli charakterystyki danych.")
        return

    export_table(
        df=table,
        directory=DATASETS_TABLE_DIR,
        filename="dataset_impact",
        caption=(
            "Średni czas wykonania badanych algorytmów dla różnych "
            "charakterystyk danych wejściowych przy 8 jednostkach wykonawczych."
        ),
        label="tab:dataset-impact",
        column_format="p{5.2cm}rrrr",
    )