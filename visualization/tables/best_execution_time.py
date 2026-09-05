import pandas as pd

from visualization.config import DEFAULT_DATASET, DEFAULT_DATA_SIZE, BEST_EXECUTION_TIME_TABLE_DIR
from visualization.loader import load_all
from visualization.filters import filter_dataset, filter_data_size, filter_parallel, filter_sequential
from visualization.tables.common import export_table, format_mean_std


def build_best_execution_time_table(
    df: pd.DataFrame,
    dataset: str = DEFAULT_DATASET,
    data_size: int = DEFAULT_DATA_SIZE,
) -> pd.DataFrame:

    filtered = filter_dataset(df, dataset)
    filtered = filter_data_size(filtered, data_size)

    sequential = filter_sequential(filtered).copy()
    parallel = filter_parallel(filtered).copy()

    if sequential.empty or parallel.empty:
        return pd.DataFrame()

    rows = []

    algorithms = sorted(
        set(sequential["algorithm"]).intersection(
            set(parallel["algorithm"])
        )
    )

    for algorithm in algorithms:
        seq_df = sequential[
            sequential["algorithm"] == algorithm
        ]

        par_df = parallel[
            parallel["algorithm"] == algorithm
        ]

        if seq_df.empty or par_df.empty:
            continue

        sequential_time = seq_df["avg_time"].iloc[0]
        sequential_std = seq_df["std_time"].iloc[0]

        best_parallel_row = par_df.loc[
            par_df["avg_time"].idxmin()
        ]

        best_parallel_time = best_parallel_row["avg_time"]
        best_parallel_std = best_parallel_row["std_time"]
        best_cores = int(best_parallel_row["cores"])
        speedup = best_parallel_row["speedup"]

        rows.append({
            "Algorytm": algorithm,
            "Czas sekw. [s]": format_mean_std(
                sequential_time,
                sequential_std,
            ),
            "Najlepszy równ. [s]": format_mean_std(
                best_parallel_time,
                best_parallel_std,
            ),
            "Jednostki": best_cores,
            "Przyspieszenie": speedup,
        })

    result = pd.DataFrame(rows)

    if result.empty:
        return result

    preferred_order = [
        "Quick Sort",
        "Merge Sort",
        "Bucket Sort",
        "Sample Sort",
    ]

    result["Algorytm"] = pd.Categorical(
        result["Algorytm"],
        categories=preferred_order,
        ordered=True,
    )

    result = result.sort_values(
        "Algorytm"
    ).reset_index(drop=True)

    result["Algorytm"] = result["Algorytm"].astype(str)

    return result


def generate_best_execution_time_table() -> None:
    print()
    print(">>> Generowanie tabeli najlepszych czasów wykonania...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    table = build_best_execution_time_table(df)

    if table.empty:
        print("Brak danych do wygenerowania tabeli.")
        return

    export_table(
        df=table,
        directory=BEST_EXECUTION_TIME_TABLE_DIR,
        filename="best_execution_time",
        caption=(
            "Najlepsze czasy wariantów równoległych w porównaniu "
            "z wariantami sekwencyjnymi dla 1~000~000 elementów "
            "zbioru random\\_int."
        ),
        label="tab:best-execution-time",
        column_format="lrrrr",
    )

    print(">>> Zakończono generowanie tabeli najlepszych czasów.")