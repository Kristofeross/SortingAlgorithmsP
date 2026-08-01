import numpy as np

from visualization.config import (
    ALGORITHM_COLORS, DATASET_LABELS,
    DATASET_IMPACT_SETS, SORTEDNESS_SETS,
    DEFAULT_DATA_SIZE, DEFAULT_CORES,
    DATASETS_IMPACT_DIR, DATASETS_SORTEDNESS_DIR,
)
from visualization.loader import load_all, get_algorithms, get_data_sizes
from visualization.utils import create_figure, finish_plot
from visualization.filters import filter_algorithm, filter_dataset, filter_parallel


# Charts: influence of input data nature
def _resolve_data_size(df, preferred: int) -> int | None:
    available = get_data_sizes()

    if not available:
        return None

    if preferred in available:
        return preferred

    return max(available)


# Chart: Impact of data type (random/duplicate, int/float)
def plot_dataset_impact(df, data_size: int = DEFAULT_DATA_SIZE, cores: int = DEFAULT_CORES):
    print("Generowanie: Dataset Impact")

    resolved_size = _resolve_data_size(df, data_size)

    if resolved_size is None:
        print("  Brak danych, pomijam.")
        return

    algorithms = get_algorithms()

    datasets = [d for d in DATASET_IMPACT_SETS if not filter_dataset(df, d).empty]

    if not datasets:
        print("Brak zbiorów z DATASET_IMPACT_SETS w bazie, pomijam.")
        return

    subset_df = df[df["data_size"] == resolved_size]
    subset_df = subset_df[subset_df["dataset"].isin(datasets)]
    subset_df = filter_parallel(subset_df)
    subset_df = subset_df[subset_df["cores"] == cores]

    if subset_df.empty:
        print(f"Brak danych dla rozmiaru={resolved_size}, rdzeni={cores}, pomijam.")
        return

    fig, ax = create_figure()

    x = np.arange(len(datasets))
    n_algorithms = len(algorithms)
    bar_width = 0.8 / max(n_algorithms, 1)

    for i, algorithm in enumerate(algorithms):
        algorithm_df = filter_algorithm(subset_df, algorithm)

        values = []
        for dataset in datasets:
            row = algorithm_df[algorithm_df["dataset"] == dataset]
            values.append(row["avg_time"].mean() if not row.empty else np.nan)

        offset = (i - (n_algorithms - 1) / 2) * bar_width

        ax.bar(
            x + offset,
            values,
            width=bar_width,
            label=algorithm,
            color=ALGORITHM_COLORS.get(algorithm),
        )

    ax.set_xticks(x)
    ax.set_xticklabels([DATASET_LABELS.get(d, d) for d in datasets])
    ax.tick_params(axis="x", rotation=20)

    ax.set_title(
        f"Wpływ rodzaju danych wejściowych na czas wykonania\n"
        f"{resolved_size:,} elementów, {cores} rdzeni"
    )
    ax.set_xlabel("Rodzaj danych")
    ax.set_ylabel("Czas wykonania [s]")

    filename = f"dataset_impact_{resolved_size}_{cores}cores"

    finish_plot(fig, ax, DATASETS_IMPACT_DIR, filename)


# Chart: Impact of input sorting (0/20/40/60/80%) on execution time - separate graphs for int and float
def plot_sortedness_impact(df, data_size: int = DEFAULT_DATA_SIZE, cores: int = DEFAULT_CORES):
    print("Generowanie: Sortedness Impact")

    resolved_size = _resolve_data_size(df, data_size)

    if resolved_size is None:
        print("  Brak danych, pomijam.")
        return

    algorithms = get_algorithms()

    for data_type, sortedness_sets in SORTEDNESS_SETS.items():
        points = [
            (percent, dataset)
            for percent, dataset in sortedness_sets
            if not filter_dataset(df, dataset).empty
        ]

        if not points:
            print(f"  Brak zbiorów sortedness dla typu '{data_type}', pomijam.")
            continue

        dataset_names = [dataset for _, dataset in points]

        subset_df = df[df["data_size"] == resolved_size]
        subset_df = subset_df[subset_df["dataset"].isin(dataset_names)]
        subset_df = filter_parallel(subset_df)
        subset_df = subset_df[subset_df["cores"] == cores]

        if subset_df.empty:
            print(f"  Brak danych dla typu '{data_type}' przy rozmiarze={resolved_size}, "
                  f"rdzeni={cores}, pomijam.")
            continue

        fig, ax = create_figure()

        for algorithm in algorithms:
            algorithm_df = filter_algorithm(subset_df, algorithm)

            percents = []
            times = []

            for percent, dataset in points:
                row = algorithm_df[algorithm_df["dataset"] == dataset]

                if row.empty:
                    continue

                percents.append(percent)
                times.append(row["avg_time"].mean())

            if not percents:
                continue

            ax.plot(
                percents,
                times,
                label=algorithm,
                color=ALGORITHM_COLORS.get(algorithm),
                marker="o",
            )

        ax.set_title(
            f"Wpływ stopnia posortowania danych ({data_type})\n"
            f"{resolved_size:,} elementów, {cores} rdzeni"
        )
        ax.set_xlabel("Stopień posortowania danych [%]")
        ax.set_ylabel("Czas wykonania [s]")

        filename = f"sortedness_impact_{data_type}_{resolved_size}_{cores}cores"

        finish_plot(fig, ax, DATASETS_SORTEDNESS_DIR, filename)


def generate_all_dataset_charts() -> None:
    print()
    print(">>> Generowanie wykresów wpływu danych wejściowych...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    charts = [
        ("Dataset Impact", plot_dataset_impact),
        ("Sortedness Impact", plot_sortedness_impact),
    ]

    for name, function in charts:
        print()
        print(f"--- {name} ---")
        function(df)

    print()
    print(">>> Zakończono generowanie wykresów wpływu danych wejściowych")
