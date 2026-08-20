from visualization.config import (
    ALGORITHM_COLORS, ALGORITHM_MARKERS, DATASET_LABELS, DEFAULT_DATASET,
    EFFICIENCY_VS_DATA_SIZE_DIR, EFFICIENCY_VS_CORES_DIR, EFFICIENCY_COMPARISON_DIR,
)
from visualization.loader import load_all, get_algorithms, get_data_sizes, get_datasets
from visualization.utils import create_figure, finish_plot, get_distinct_colors
from visualization.style import use_log_scale_x, set_clean_ticks
from visualization.filters import filter_algorithm, filter_dataset, filter_parallel


def plot_ideal_efficiency_line(ax) -> None:
    ax.axhline(
        y=1.0,
        linestyle="--",
        color="gray",
        linewidth=1.5,
        label="Efficiency idealna",
        zorder=1,
    )


def plot_efficiency_vs_cores_per_algorithm(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Efficiency vs Cores (per algorytm)")

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

        plot_ideal_efficiency_line(ax)

        for i, size in enumerate(data_sizes):
            size_df = algorithm_df[algorithm_df["data_size"] == size]
            size_df = size_df.sort_values("cores")

            if size_df.empty:
                continue

            ax.plot(
                size_df["cores"],
                size_df["efficiency"],
                label=f"{size:,} elementów",
                marker="o",
                color=cmap[i],
            )

        use_log_scale_x(ax)
        set_clean_ticks(ax, dataset_df["cores"].unique())

        ax.set_title(
            f"{algorithm}\n"
            f"Efficiency w zależności od liczby rdzeni\n"
            f"{dataset_label}"
        )
        ax.set_xlabel("Liczba rdzeni")
        ax.set_ylabel("Efficiency")

        filename = algorithm.lower().replace(" ", "_") + "_efficiency_vs_cores_" + dataset

        finish_plot(fig, ax, EFFICIENCY_VS_CORES_DIR, filename, subfolder=dataset)


def plot_efficiency_vs_cores_comparison(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Efficiency vs Cores (porównanie algorytmów)")

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

    plot_ideal_efficiency_line(ax)

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)
        algorithm_df = algorithm_df.sort_values("cores")

        if algorithm_df.empty:
            continue

        ax.plot(
            algorithm_df["cores"],
            algorithm_df["efficiency"],
            label=algorithm,
            color=ALGORITHM_COLORS.get(algorithm),
            marker=ALGORITHM_MARKERS.get(algorithm),
        )

    use_log_scale_x(ax)
    set_clean_ticks(ax, dataset_df["cores"].unique())

    ax.set_title(
        f"Efficiency w zależności od liczby rdzeni\n"
        f"{dataset_label}, {max_size:,} elementów"
    )
    ax.set_xlabel("Liczba rdzeni")
    ax.set_ylabel("Efficiency")

    filename = "efficiency_vs_cores_comparison_" + dataset

    finish_plot(fig, ax, EFFICIENCY_VS_CORES_DIR, filename, subfolder=dataset)


def plot_efficiency_vs_data_size(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Efficiency vs Data Size")

    algorithms = get_algorithms()

    dataset_df = filter_dataset(df, dataset)
    dataset_df = filter_parallel(dataset_df)

    if dataset_df.empty:
        print(f"  Brak danych dla zbioru '{dataset}', pomijam.")
        return

    max_cores = dataset_df["cores"].max()
    dataset_df = dataset_df[dataset_df["cores"] == max_cores]

    dataset_label = DATASET_LABELS.get(dataset, dataset)

    fig, ax = create_figure()

    plot_ideal_efficiency_line(ax)

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)
        algorithm_df = algorithm_df.sort_values("data_size")

        if algorithm_df.empty:
            continue

        ax.plot(
            algorithm_df["data_size"],
            algorithm_df["efficiency"],
            label=algorithm,
            color=ALGORITHM_COLORS.get(algorithm),
            marker=ALGORITHM_MARKERS.get(algorithm),
        )

    use_log_scale_x(ax)
    set_clean_ticks(ax, dataset_df["data_size"].unique())

    ax.set_title(
        f"Efficiency w zależności od rozmiaru danych\n"
        f"{dataset_label}, {max_cores} rdzeni"
    )
    ax.set_xlabel("Rozmiar danych")
    ax.set_ylabel("Efficiency")

    filename = "efficiency_vs_data_size_" + dataset

    finish_plot(fig, ax, EFFICIENCY_VS_DATA_SIZE_DIR, filename, subfolder=dataset)


def plot_efficiency_comparison(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Efficiency Comparison (ranking)")

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
            ["efficiency"]
            .mean()
            .sort_values("efficiency", ascending=False)
        )

        if comparison_df.empty:
            continue

        fig, ax = create_figure()

        plot_ideal_efficiency_line(ax)

        colors = [
            ALGORITHM_COLORS.get(algorithm)
            for algorithm in comparison_df["algorithm"]
        ]

        ax.bar(
            comparison_df["algorithm"],
            comparison_df["efficiency"],
            color=colors,
        )

        ax.set_title(
            f"Ranking algorytmów wg efficiency\n"
            f"{dataset_label}, {size:,} elementów, {max_cores} rdzeni"
        )
        ax.set_xlabel("Algorytm")
        ax.set_ylabel("Efficiency")
        ax.tick_params(axis="x", rotation=20)

        filename = f"efficiency_comparison_{dataset}_{size}"

        finish_plot(fig, ax, EFFICIENCY_COMPARISON_DIR, filename, subfolder=dataset)


def generate_all_efficiency_charts() -> None:
    print()
    print(">>> Generowanie wykresów efficiency...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    charts = [
        ("Efficiency vs Cores (per algorytm)", plot_efficiency_vs_cores_per_algorithm),
        ("Efficiency vs Cores (porównanie)", plot_efficiency_vs_cores_comparison),
        ("Efficiency vs Data Size", plot_efficiency_vs_data_size),
        ("Efficiency Comparison (ranking)", plot_efficiency_comparison),
    ]

    datasets = get_datasets()

    for name, function in charts:
        print()
        print(f"--- {name} ---")

        for dataset in datasets:
            function(df, dataset=dataset)

    print()
    print(">>> Zakończono generowanie wykresów efficiency")