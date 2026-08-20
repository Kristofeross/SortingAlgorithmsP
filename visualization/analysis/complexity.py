import numpy as np
import matplotlib.pyplot as plt

from visualization.config import (
    ALGORITHM_COLORS, ALGORITHM_MARKERS, DATASET_LABELS,
    DEFAULT_DATASET, COMPLEXITY_TABLES_DIR, COMPLEXITY_DIR,
)
from visualization.loader import load_all, get_algorithms, get_datasets
from visualization.utils import ensure_directory
from visualization.style import set_clean_ticks, format_log_axis_plain
from visualization.filters import filter_algorithm, filter_dataset, filter_sequential


THEORETICAL_COMPLEXITY = {
    "Quick Sort": {
        "description": "O(n log n) średnio, O(n^2) w najgorszym przypadku",
        "expected_range": (1.0, 1.3),
    },
    "Merge Sort": {
        "description": "O(n log n) zawsze (najlepszy = najgorszy przypadek)",
        "expected_range": (1.0, 1.3),
    },
    "Bucket Sort": {
        "description": "O(n + k) średnio przy równomiernym rozkładzie danych, O(n^2) w najgorszym przypadku",
        "expected_range": (0.9, 1.3),
    },
    "Sample Sort": {
        "description": "O(n log n) średnio",
        "expected_range": (1.0, 1.3),
    },
}


def fit_complexity(df, dataset: str = DEFAULT_DATASET) -> "pd.DataFrame":
    import pandas as pd

    algorithms = get_algorithms()
    dataset_df = filter_dataset(df, dataset)
    dataset_df = filter_sequential(dataset_df)

    rows = []

    for algorithm in algorithms:
        algo_df = filter_algorithm(dataset_df, algorithm).sort_values("data_size")

        if len(algo_df) < 2:
            continue

        log_n = np.log(algo_df["data_size"].values)
        log_t = np.log(algo_df["avg_time"].values)

        slope, intercept = np.polyfit(log_n, log_t, 1)

        predicted = slope * log_n + intercept
        ss_res = np.sum((log_t - predicted) ** 2)
        ss_tot = np.sum((log_t - log_t.mean()) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else float("nan")

        info = THEORETICAL_COMPLEXITY.get(algorithm, {})
        expected_range = info.get("expected_range")

        if expected_range:
            in_range = expected_range[0] <= slope <= expected_range[1]
        else:
            in_range = None

        rows.append({
            "dataset": dataset,
            "algorithm": algorithm,
            "empirical_exponent": slope,
            "intercept": intercept,
            "r_squared": r_squared,
            "theoretical_complexity": info.get("description", "-"),
            "expected_exponent_range": (
                f"{expected_range[0]}-{expected_range[1]}" if expected_range else "-"
            ),
            "zgodne_z_teoria": in_range,
        })

    return pd.DataFrame(rows)


def export_complexity_table(complexity_df) -> None:
    ensure_directory(COMPLEXITY_TABLES_DIR)
    path = COMPLEXITY_TABLES_DIR / "complexity_analysis.csv"
    complexity_df.to_csv(path, index=False, float_format="%.4f")
    print(f"  Zapisano: {path}")


def plot_complexity_fit(df, complexity_df, dataset: str = DEFAULT_DATASET) -> None:
    algorithms = get_algorithms()
    dataset_df = filter_dataset(df, dataset)
    dataset_df = filter_sequential(dataset_df)
    dataset_label = DATASET_LABELS.get(dataset, dataset)

    fig, ax = plt.subplots(figsize=(10, 7))

    for algorithm in algorithms:
        algo_df = filter_algorithm(dataset_df, algorithm).sort_values("data_size")

        if algo_df.empty:
            continue

        fit_row = complexity_df[complexity_df["algorithm"] == algorithm]

        color = ALGORITHM_COLORS.get(algorithm)
        marker = ALGORITHM_MARKERS.get(algorithm)

        ax.scatter(
            algo_df["data_size"], algo_df["avg_time"],
            color=color, marker=marker, s=70, zorder=3,
        )

        if not fit_row.empty:
            slope = fit_row["empirical_exponent"].iloc[0]
            intercept = fit_row["intercept"].iloc[0]

            n_range = np.array([algo_df["data_size"].min(), algo_df["data_size"].max()])
            fitted_time = np.exp(intercept) * n_range ** slope

            ax.plot(
                n_range, fitted_time,
                color=color, linestyle="--", linewidth=1.5,
                label=f"{algorithm} (a={slope:.2f})",
            )

    ax.set_xscale("log")
    ax.set_yscale("log")
    set_clean_ticks(ax, dataset_df["data_size"].unique())
    format_log_axis_plain(ax)

    ax.set_title(
        f"Empiryczna złożoność obliczeniowa (dane sekwencyjne)\n"
        f"{dataset_label}"
    )
    ax.set_xlabel("Rozmiar danych (n)")
    ax.set_ylabel("Czas wykonania [s]")
    ax.legend(fontsize=10)

    ensure_directory(COMPLEXITY_DIR / dataset)
    path = COMPLEXITY_DIR / dataset / f"complexity_fit_{dataset}.png"
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  Zapisano: {path}")


def generate_complexity_analysis() -> None:
    print()
    print(">>> Generowanie analizy złożoności...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    all_fits = []

    for dataset in get_datasets():
        complexity_df = fit_complexity(df, dataset=dataset)

        if complexity_df.empty:
            print(f"  Brak wystarczających danych sekwencyjnych dla '{dataset}', pomijam.")
            continue

        all_fits.append(complexity_df)
        plot_complexity_fit(df, complexity_df, dataset=dataset)

    if not all_fits:
        print("Brak danych do zbudowania tabeli złożoności.")
        return

    import pandas as pd
    combined = pd.concat(all_fits, ignore_index=True)
    export_complexity_table(combined)

    print(">>> Zakończono generowanie analizy złożoności")