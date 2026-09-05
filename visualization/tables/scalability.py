import pandas as pd

from visualization.config import DEFAULT_DATASET, DEFAULT_DATA_SIZE, SCALABILITY_TABLE_DIR
from visualization.loader import load_all
from visualization.filters import filter_dataset, filter_data_size, filter_parallel
from visualization.tables.common import export_table, format_mean_std


def build_scalability_base(
    df: pd.DataFrame,
    dataset: str = DEFAULT_DATASET,
    data_size: int = DEFAULT_DATA_SIZE,
) -> pd.DataFrame:

    filtered = filter_dataset(df, dataset)
    filtered = filter_data_size(filtered, data_size)
    filtered = filter_parallel(filtered)

    return filtered.copy()


def build_metric_pivot(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    pivot = df.pivot_table(index="cores", columns="algorithm", values=metric, aggfunc="mean")

    pivot = pivot.reset_index()

    pivot = pivot.rename(
        columns={"cores": "Jednostki wykonawcze"}
    )

    preferred_order = ["Jednostki wykonawcze", "Quick Sort", "Merge Sort", "Bucket Sort", "Sample Sort"]

    available = [
        column
        for column in preferred_order
        if column in pivot.columns
    ]

    return pivot[available]


def build_time_pivot(df: pd.DataFrame) -> pd.DataFrame:
    formatted = df.copy()

    formatted["time_with_std"] = formatted.apply(
        lambda row: format_mean_std(
            row["avg_time"],
            row["std_time"],
        ),
        axis=1,
    )

    pivot = formatted.pivot_table(index="cores", columns="algorithm", values="time_with_std", aggfunc="first")

    pivot = pivot.reset_index()

    pivot = pivot.rename(
        columns={
            "cores": "Jednostki wykonawcze",
            "Quick Sort": "Quick Sort [s]",
            "Merge Sort": "Merge Sort [s]",
            "Bucket Sort": "Bucket Sort [s]",
            "Sample Sort": "Sample Sort [s]",
        }
    )

    preferred_order = ["Jednostki wykonawcze", "Quick Sort [s]", "Merge Sort [s]", "Bucket Sort [s]", "Sample Sort [s]"]

    available = [
        column
        for column in preferred_order
        if column in pivot.columns
    ]

    return pivot[available]


def generate_scalability_tables() -> None:
    print()
    print(">>> Generowanie tabel skalowalności...")

    df = load_all()

    base = build_scalability_base(df)

    if base.empty:
        print("Brak danych do tabel skalowalności.")
        return

    time_table = build_time_pivot(base)
    speedup_table = build_metric_pivot(base, "speedup")
    efficiency_table = build_metric_pivot(base, "efficiency")

    export_table(
        df=time_table,
        directory=SCALABILITY_TABLE_DIR,
        filename="scalability_time",
        caption=(
            "Średni czas wykonania wraz z odchyleniem standardowym "
            "badanych algorytmów w zależności od liczby jednostek wykonawczych."
        ),
        label="tab:scalability-time",
        column_format="p{2.5cm}rrrr",
    )

    export_table(
        df=speedup_table,
        directory=SCALABILITY_TABLE_DIR,
        filename="scalability_speedup",
        caption=(
            "Przyspieszenie badanych algorytmów w zależności "
            "od liczby jednostek wykonawczych."
        ),
        label="tab:scalability-speedup",
        column_format="p{2.5cm}rrrr",
    )

    export_table(
        df=efficiency_table,
        directory=SCALABILITY_TABLE_DIR,
        filename="scalability_efficiency",
        caption=(
            "Efektywność badanych algorytmów w zależności "
            "od liczby jednostek wykonawczych."
        ),
        label="tab:scalability-efficiency",
        column_format="p{2.5cm}rrrr",
    )

    print(">>> Zakończono generowanie tabel skalowalności.")