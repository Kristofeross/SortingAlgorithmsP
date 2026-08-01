import numpy as np

from visualization.config import (
    ALGORITHM_COLORS, ALGORITHM_MARKERS, DATASET_LABELS,DEFAULT_DATASET,
    MEMORY_VS_DATA_SIZE_DIR, MEMORY_VS_CORES_DIR, MEMORY_COMPARISON_DIR,
)
from visualization.loader import load_all, get_algorithms, get_data_sizes
from visualization.utils import create_figure, finish_plot, get_distinct_colors
from visualization.style import use_log_scale_x, set_clean_ticks
from visualization.filters import filter_algorithm, filter_dataset, filter_parallel


# Chart: RAM usage (avg_mem) vs. number of cores, separate file per algorithm, separate line per data size.
def plot_memory_vs_cores_per_algorithm(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Memory vs Cores (per algorytm)")

    algorithms = get_algorithms()
    data_sizes = sorted(get_data_sizes())

    dataset_df = filter_dataset(df, dataset)
    dataset_df = filter_parallel(dataset_df)

    if dataset_df.empty:
        print(f"  Brak danych dla zbioru '{dataset}', pomijam.")
        return

    dataset_label = DATASET_LABELS.get(dataset, dataset)
    cmap = get_distinct_colors(len(data_sizes))

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)

        if algorithm_df.empty:
            continue

        fig, ax = create_figure()

        for i, size in enumerate(data_sizes):
            size_df = algorithm_df[algorithm_df["data_size"] == size]
            size_df = size_df.sort_values("cores")

            if size_df.empty:
                continue

            ax.plot(
                size_df["cores"],
                size_df["avg_mem"],
                label=f"{size:,} elementów",
                marker="o",
                color=cmap[i],
            )

        use_log_scale_x(ax)
        set_clean_ticks(ax, dataset_df["cores"].unique())

        ax.set_title(
            f"{algorithm}\n"
            f"Średnie zużycie RAM w zależności od liczby rdzeni\n"
            f"{dataset_label}"
        )
        ax.set_xlabel("Liczba rdzeni")
        ax.set_ylabel("Średnie użycie RAM [MB]")

        filename = algorithm.lower().replace(" ", "_") + "_memory_vs_cores_" + dataset

        finish_plot(fig, ax, MEMORY_VS_CORES_DIR, filename)


# Chart: RAM usage vs. number of cores, all algorithms in one chart, for the largest available data size
def plot_memory_vs_cores_comparison(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Memory vs Cores (porównanie algorytmów)")

    algorithms = get_algorithms()
    data_sizes = get_data_sizes()

    if not data_sizes:
        return

    max_size = max(data_sizes)

    dataset_df = filter_dataset(df, dataset)
    dataset_df = dataset_df[dataset_df["data_size"] == max_size]
    dataset_df = filter_parallel(dataset_df)

    if dataset_df.empty:
        print(f"  Brak danych dla zbioru '{dataset}', pomijam.")
        return

    dataset_label = DATASET_LABELS.get(dataset, dataset)

    fig, ax = create_figure()

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)
        algorithm_df = algorithm_df.sort_values("cores")

        if algorithm_df.empty:
            continue

        color = ALGORITHM_COLORS.get(algorithm)
        marker = ALGORITHM_MARKERS.get(algorithm)

        ax.plot(
            algorithm_df["cores"],
            algorithm_df["avg_mem"],
            label=f"{algorithm} (średnie)",
            color=color,
            marker=marker,
            linestyle="-",
        )

        ax.plot(
            algorithm_df["cores"],
            algorithm_df["max_mem"],
            label=f"{algorithm} (max)",
            color=color,
            marker=marker,
            linestyle="--",
            alpha=0.6,
        )

    use_log_scale_x(ax)
    set_clean_ticks(ax, dataset_df["cores"].unique())

    ax.set_title(
        f"Zużycie RAM w zależności od liczby rdzeni\n"
        f"{dataset_label}, {max_size:,} elementów"
    )
    ax.set_xlabel("Liczba rdzeni")
    ax.set_ylabel("Użycie RAM [MB]")
    ax.legend(fontsize=9, ncol=2)

    filename = "memory_vs_cores_comparison_" + dataset

    finish_plot(fig, ax, MEMORY_VS_CORES_DIR, filename)


# Chart: RAM usage vs data size, to determine the (maximum) # of cores.
def plot_memory_vs_data_size(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Memory vs Data Size")

    algorithms = get_algorithms()

    dataset_df = filter_dataset(df, dataset)
    dataset_df = filter_parallel(dataset_df)

    if dataset_df.empty:
        print(f"Brak danych dla zbioru '{dataset}', pomijam.")
        return

    max_cores = dataset_df["cores"].max()
    dataset_df = dataset_df[dataset_df["cores"] == max_cores]

    dataset_label = DATASET_LABELS.get(dataset, dataset)

    fig, ax = create_figure()

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)
        algorithm_df = algorithm_df.sort_values("data_size")

        if algorithm_df.empty:
            continue

        color = ALGORITHM_COLORS.get(algorithm)
        marker = ALGORITHM_MARKERS.get(algorithm)

        ax.plot(
            algorithm_df["data_size"],
            algorithm_df["avg_mem"],
            label=f"{algorithm} (średnie)",
            color=color,
            marker=marker,
            linestyle="-",
        )

        ax.plot(
            algorithm_df["data_size"],
            algorithm_df["max_mem"],
            label=f"{algorithm} (max)",
            color=color,
            marker=marker,
            linestyle="--",
            alpha=0.6,
        )

    use_log_scale_x(ax)
    set_clean_ticks(ax, dataset_df["data_size"].unique())

    ax.set_title(
        f"Zużycie RAM w zależności od rozmiaru danych\n"
        f"{dataset_label}, {max_cores} rdzeni"
    )
    ax.set_xlabel("Rozmiar danych")
    ax.set_ylabel("Użycie RAM [MB]")
    ax.legend(fontsize=9, ncol=2)

    filename = "memory_vs_data_size_" + dataset

    finish_plot(fig, ax, MEMORY_VS_DATA_SIZE_DIR, filename)


# Chart: Ranking of algorithms by RAM usage, for each size
def plot_memory_comparison(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Memory Comparison (ranking)")

    data_sizes = sorted(get_data_sizes())

    if not data_sizes:
        return

    dataset_df = filter_dataset(df, dataset)
    dataset_label = DATASET_LABELS.get(dataset, dataset)

    for size in data_sizes:
        size_df = dataset_df[dataset_df["data_size"] == size]
        size_df = filter_parallel(size_df)

        if size_df.empty:
            continue

        max_cores = size_df["cores"].max()
        size_df = size_df[size_df["cores"] == max_cores]

        comparison_df = (
            size_df
            .groupby("algorithm", as_index=False)
            [["avg_mem", "max_mem"]]
            .mean()
            .sort_values("avg_mem")
        )

        if comparison_df.empty:
            continue

        fig, ax = create_figure()

        x = np.arange(len(comparison_df))
        bar_width = 0.35

        ax.bar(
            x - bar_width / 2,
            comparison_df["avg_mem"],
            width=bar_width,
            label="Średnie RAM",
            color="tab:blue",
        )

        ax.bar(
            x + bar_width / 2,
            comparison_df["max_mem"],
            width=bar_width,
            label="Maksymalne RAM",
            color="tab:blue",
            alpha=0.5,
            hatch="//",
        )

        ax.set_xticks(x)
        ax.set_xticklabels(comparison_df["algorithm"])
        ax.tick_params(axis="x", rotation=20)

        ax.set_title(
            f"Ranking algorytmów wg zużycia RAM\n"
            f"{dataset_label}, {size:,} elementów, {max_cores} rdzeni"
        )
        ax.set_xlabel("Algorytm")
        ax.set_ylabel("Użycie RAM [MB]")

        filename = f"memory_comparison_{dataset}_{size}"

        finish_plot(fig, ax, MEMORY_COMPARISON_DIR, filename)


def generate_all_memory_charts() -> None:
    print()
    print(">>> Generowanie wykresów zużycia RAM...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    charts = [
        ("Memory vs Cores (na 1 algorytm)", plot_memory_vs_cores_per_algorithm),
        ("Memory vs Cores (porównanie)", plot_memory_vs_cores_comparison),
        ("Memory vs Data Size", plot_memory_vs_data_size),
        ("Memory Comparison (ranking)", plot_memory_comparison),
    ]

    for name, function in charts:
        print()
        print(f"--- {name} ---")
        function(df)

    print()
    print(">>> Zakończono generowanie wykresów zużycia RAM")