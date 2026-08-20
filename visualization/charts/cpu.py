from visualization.config import (
    ALGORITHM_COLORS, ALGORITHM_MARKERS, DATASET_LABELS, DEFAULT_DATASET,
    CPU_VS_DATA_SIZE_DIR, CPU_VS_CORES_DIR, CPU_COMPARISON_DIR,
)
from visualization.loader import load_all, get_algorithms, get_data_sizes, get_datasets
from visualization.utils import create_figure, finish_plot, get_distinct_colors
from visualization.style import use_log_scale_x, set_clean_ticks
from visualization.filters import filter_algorithm, filter_dataset, filter_parallel


def plot_ideal_cpu_line(ax, cores) -> None:
    cores_sorted = sorted(set(cores))
    ideal = [c * 100 for c in cores_sorted]

    ax.plot(
        cores_sorted,
        ideal,
        linestyle="--",
        color="gray",
        linewidth=1.5,
        label="Maksymalne możliwe CPU (rdzenie × 100%)",
        zorder=1,
    )


def plot_cpu_vs_cores_per_algorithm(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: CPU vs Cores (per algorytm)")

    algorithms = get_algorithms()
    data_sizes = sorted(get_data_sizes())

    dataset_df = filter_dataset(df, dataset)
    dataset_df = filter_parallel(dataset_df)

    if dataset_df.empty:
        print(f"  Brak danych dla zbioru '{dataset}', pomijam.")
        return

    dataset_label = DATASET_LABELS.get(dataset, dataset)
    all_cores = sorted(dataset_df["cores"].unique())
    cmap = get_distinct_colors(len(data_sizes))

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)

        if algorithm_df.empty:
            continue

        fig, ax = create_figure()

        plot_ideal_cpu_line(ax, all_cores)

        for i, size in enumerate(data_sizes):
            size_df = algorithm_df[algorithm_df["data_size"] == size]
            size_df = size_df.sort_values("cores")

            if size_df.empty:
                continue

            ax.plot(
                size_df["cores"],
                size_df["avg_cpu"],
                label=f"{size:,} elementów",
                marker="o",
                color=cmap[i],
            )

        use_log_scale_x(ax)
        set_clean_ticks(ax, all_cores)

        ax.set_title(
            f"{algorithm}\n"
            f"Wykorzystanie CPU w zależności od liczby rdzeni\n"
            f"{dataset_label}"
        )
        ax.set_xlabel("Liczba rdzeni")
        ax.set_ylabel("Średnie użycie CPU [%]")

        filename = algorithm.lower().replace(" ", "_") + "_cpu_vs_cores_" + dataset

        finish_plot(fig, ax, CPU_VS_CORES_DIR, filename, subfolder=dataset)


def plot_cpu_vs_cores_comparison(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: CPU vs Cores (porównanie algorytmów)")

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
    all_cores = sorted(dataset_df["cores"].unique())

    fig, ax = create_figure()

    plot_ideal_cpu_line(ax, all_cores)

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)
        algorithm_df = algorithm_df.sort_values("cores")

        if algorithm_df.empty:
            continue

        ax.plot(
            algorithm_df["cores"],
            algorithm_df["avg_cpu"],
            label=algorithm,
            color=ALGORITHM_COLORS.get(algorithm),
            marker=ALGORITHM_MARKERS.get(algorithm),
        )

    use_log_scale_x(ax)
    set_clean_ticks(ax, all_cores)

    ax.set_title(
        f"Wykorzystanie CPU w zależności od liczby rdzeni\n"
        f"{dataset_label}, {max_size:,} elementów"
    )
    ax.set_xlabel("Liczba rdzeni")
    ax.set_ylabel("Średnie użycie CPU [%]")

    filename = "cpu_vs_cores_comparison_" + dataset

    finish_plot(fig, ax, CPU_VS_CORES_DIR, filename, subfolder=dataset)


def plot_cpu_vs_data_size(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: CPU vs Data Size")

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

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)
        algorithm_df = algorithm_df.sort_values("data_size")

        if algorithm_df.empty:
            continue

        ax.plot(
            algorithm_df["data_size"],
            algorithm_df["avg_cpu"],
            label=algorithm,
            color=ALGORITHM_COLORS.get(algorithm),
            marker=ALGORITHM_MARKERS.get(algorithm),
        )

    use_log_scale_x(ax)
    set_clean_ticks(ax, dataset_df["data_size"].unique())

    ax.axhline(
        y=max_cores * 100,
        linestyle="--",
        color="gray",
        linewidth=1.5,
        label=f"Maksymalne możliwe CPU ({max_cores} × 100%)",
    )

    ax.set_title(
        f"Wykorzystanie CPU w zależności od rozmiaru danych\n"
        f"{dataset_label}, {max_cores} rdzeni"
    )
    ax.set_xlabel("Rozmiar danych")
    ax.set_ylabel("Średnie użycie CPU [%]")

    filename = "cpu_vs_data_size_" + dataset

    finish_plot(fig, ax, CPU_VS_DATA_SIZE_DIR, filename, subfolder=dataset)


def plot_cpu_comparison(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: CPU Comparison (ranking)")

    dataset_df = filter_dataset(df, dataset)
    dataset_df = filter_parallel(dataset_df)
    dataset_label = DATASET_LABELS.get(dataset, dataset)

    if dataset_df.empty:
        return

    combos = (
        dataset_df[["data_size", "cores"]]
        .drop_duplicates()
        .sort_values(["data_size", "cores"])
        .itertuples(index=False)
    )

    for size, cores in combos:
        combo_df = dataset_df[
            (dataset_df["data_size"] == size) & (dataset_df["cores"] == cores)
        ]

        comparison_df = (
            combo_df
            .groupby("algorithm", as_index=False)
            ["avg_cpu"]
            .mean()
            .sort_values("avg_cpu", ascending=False)
        )

        if comparison_df.empty:
            continue

        fig, ax = create_figure()

        ax.axhline(
            y=cores * 100,
            linestyle="--",
            color="gray",
            linewidth=1.5,
        )

        colors = [
            ALGORITHM_COLORS.get(algorithm)
            for algorithm in comparison_df["algorithm"]
        ]

        ax.bar(
            comparison_df["algorithm"],
            comparison_df["avg_cpu"],
            color=colors,
        )

        ax.set_title(
            f"Ranking algorytmów wg wykorzystania CPU\n"
            f"{dataset_label}, {size:,} elementów, {cores} rdzeni"
        )
        ax.set_xlabel("Algorytm")
        ax.set_ylabel("Średnie użycie CPU [%]")
        ax.tick_params(axis="x", rotation=20)

        filename = f"cpu_comparison_{dataset}_{size}_{cores}cores"

        finish_plot(
            fig, ax, CPU_COMPARISON_DIR, filename,
            subfolder=f"{dataset}/{size}",
        )


def generate_all_cpu_charts() -> None:
    print()
    print(">>> Generowanie wykresów CPU...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    charts = [
        ("CPU vs Cores (per algorytm)", plot_cpu_vs_cores_per_algorithm),
        ("CPU vs Cores (porównanie)", plot_cpu_vs_cores_comparison),
        ("CPU vs Data Size", plot_cpu_vs_data_size),
        ("CPU Comparison (ranking)", plot_cpu_comparison),
    ]

    datasets = get_datasets()

    for name, function in charts:
        print()
        print(f"--- {name} ---")

        for dataset in datasets:
            function(df, dataset=dataset)

    print()
    print(">>> Zakończono generowanie wykresów CPU")