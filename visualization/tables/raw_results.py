from visualization.config import RAW_RESULTS_DIR
from visualization.loader import load_all
from visualization.utils import ensure_directory


EXPORT_COLUMNS = [
    "algorithm",
    "mode",
    "dataset",
    "data_size",
    "cores",
    "avg_time",
    "median_time",
    "std_time",
    "avg_cpu",
    "avg_mem",
    "max_mem",
    "speedup",
    "efficiency",
    "avg_exact_cpu_time",
    "min_sample_count",
    "status",
    "correctness",
    "error_message",
]


def export_raw_results_by_dataset() -> None:
    print()
    print(">>> Eksport pełnych wyników według zbiorów danych...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    ensure_directory(RAW_RESULTS_DIR)

    available_columns = [
        column
        for column in EXPORT_COLUMNS
        if column in df.columns
    ]

    df = df[available_columns].copy()

    datasets = sorted(
        df["dataset"]
        .dropna()
        .unique()
    )

    for dataset in datasets:
        dataset_df = df[
            df["dataset"] == dataset
        ].copy()

        dataset_df = dataset_df.sort_values(
            by=["algorithm", "mode", "data_size", "cores"]
        )

        output_path = RAW_RESULTS_DIR / f"{dataset}.csv"

        dataset_df.to_csv(output_path, index=False, float_format="%.6f", encoding="utf-8-sig")

        print(
            f"  {dataset}: "
            f"{len(dataset_df)} wierszy -> "
            f"{output_path}"
        )

    print(">>> Zakończono eksport pełnych wyników.")