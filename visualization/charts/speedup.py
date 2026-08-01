from visualization.config import (
    ALGORITHM_COLORS, ALGORITHM_MARKERS, DATASET_LABELS, DEFAULT_DATASET,
    SPEEDUP_VS_DATA_SIZE_DIR, SPEEDUP_VS_CORES_DIR, SPEEDUP_COMPARISON_DIR,
)
from visualization.loader import load_all, get_algorithms, get_data_sizes
from visualization.utils import create_figure, finish_plot, get_distinct_colors
from visualization.style import use_log_scale_x, set_clean_ticks
from visualization.filters import filter_algorithm, filter_dataset, filter_parallel


def plot_ideal_speedup_line(ax, cores) -> None:
    cores_sorted = sorted(set(cores))

    ax.plot(
        cores_sorted,
        cores_sorted,
        linestyle="--",
        color="gray",
        linewidth=1.5,
        label="Speedup idealny",
        zorder=1,
    )


# Chart: speedup vs number of cores
def plot_speedup_vs_cores_per_algorithm(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Speedup vs Cores (na 1 algorytm)")

    algorithms = get_algorithms()
    data_sizes = sorted(get_data_sizes())

    dataset_df = filter_dataset(df, dataset)
    dataset_df = filter_parallel(dataset_df)

    if dataset_df.empty:
        print(f"  Brak danych dla zbioru '{dataset}', pomijam.")
        return

    dataset_label = DATASET_LABELS.get(dataset, dataset)
    all_cores = sorted(dataset_df["cores"].unique())

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)

        if algorithm_df.empty:
            continue

        fig, ax = create_figure()

        plot_ideal_speedup_line(ax, all_cores)

        cmap = get_distinct_colors(len(data_sizes))

        for i, size in enumerate(data_sizes):
            size_df = algorithm_df[algorithm_df["data_size"] == size]
            size_df = size_df.sort_values("cores")

            if size_df.empty:
                continue

            ax.plot(
                size_df["cores"],
                size_df["speedup"],
                label=f"{size:,} elementów",
                marker="o",
                color=cmap[i],
            )

        use_log_scale_x(ax)
        set_clean_ticks(ax, all_cores)

        ax.set_title(
            f"{algorithm}\n"
            f"Speedup w zależności od liczby rdzeni\n"
            f"{dataset_label}"
        )
        ax.set_xlabel("Liczba rdzeni")
        ax.set_ylabel("Speedup")

        filename = algorithm.lower().replace(" ", "_") + "_speedup_vs_cores_" + dataset

        finish_plot(fig, ax, SPEEDUP_VS_CORES_DIR, filename)


# Chart: speedup vs number of cores
def plot_speedup_vs_cores_comparison(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Speedup vs Cores (porównanie algorytmów)")

    algorithms = get_algorithms()
    data_sizes = get_data_sizes()

    if not data_sizes:
        return

    max_size = max(data_sizes)

    dataset_df = filter_dataset(df, dataset)
    dataset_df = dataset_df[dataset_df["data_size"] == max_size]
    dataset_df = filter_parallel(dataset_df)

    if dataset_df.empty:
        print(f"Brak danych dla zbioru '{dataset}', pomijam.")
        return

    dataset_label = DATASET_LABELS.get(dataset, dataset)
    all_cores = sorted(dataset_df["cores"].unique())

    fig, ax = create_figure()

    plot_ideal_speedup_line(ax, all_cores)

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)
        algorithm_df = algorithm_df.sort_values("cores")

        if algorithm_df.empty:
            continue

        ax.plot(
            algorithm_df["cores"],
            algorithm_df["speedup"],
            label=algorithm,
            color=ALGORITHM_COLORS.get(algorithm),
            marker=ALGORITHM_MARKERS.get(algorithm),
        )

    use_log_scale_x(ax)
    set_clean_ticks(ax, all_cores)

    ax.set_title(
        f"Speedup w zależności od liczby rdzeni\n"
        f"{dataset_label}, {max_size:,} elementów"
    )
    ax.set_xlabel("Liczba rdzeni")
    ax.set_ylabel("Speedup")

    filename = "speedup_vs_cores_comparison_" + dataset

    finish_plot(fig, ax, SPEEDUP_VS_CORES_DIR, filename)


# Chart: speedup vs data size, for a fixed (maximum) number of cores
def plot_speedup_vs_data_size(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Speedup vs Data Size")

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

    ax.axhline(
        y=1.0,
        linestyle="--",
        color="gray",
        linewidth=1.5,
        label="Speedup = 1 (brak zysku)",
    )

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)
        algorithm_df = algorithm_df.sort_values("data_size")

        if algorithm_df.empty:
            continue

        ax.plot(
            algorithm_df["data_size"],
            algorithm_df["speedup"],
            label=algorithm,
            color=ALGORITHM_COLORS.get(algorithm),
            marker=ALGORITHM_MARKERS.get(algorithm),
        )

    use_log_scale_x(ax)
    set_clean_ticks(ax, dataset_df["data_size"].unique())

    ax.set_title(
        f"Speedup w zależności od rozmiaru danych\n"
        f"{dataset_label}, {max_cores} rdzeni"
    )
    ax.set_xlabel("Rozmiar danych")
    ax.set_ylabel("Speedup")

    filename = "speedup_vs_data_size_" + dataset

    finish_plot(fig, ax, SPEEDUP_VS_DATA_SIZE_DIR, filename)


# Chart: ranking of algorithms by speedup, for each data size
def plot_speedup_comparison(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Speedup Comparison (ranking)")

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
            ["speedup"]
            .mean()
            .sort_values("speedup", ascending=False)
        )

        if comparison_df.empty:
            continue

        fig, ax = create_figure()

        ax.axhline(y=1.0, linestyle="--", color="gray", linewidth=1.5)

        colors = [
            ALGORITHM_COLORS.get(algorithm)
            for algorithm in comparison_df["algorithm"]
        ]

        ax.bar(
            comparison_df["algorithm"],
            comparison_df["speedup"],
            color=colors,
        )

        ax.set_title(
            f"Ranking algorytmów wg speedupu\n"
            f"{dataset_label}, {size:,} elementów, {max_cores} rdzeni"
        )
        ax.set_xlabel("Algorytm")
        ax.set_ylabel("Speedup")
        ax.tick_params(axis="x", rotation=20)

        filename = f"speedup_comparison_{dataset}_{size}"

        finish_plot(fig, ax, SPEEDUP_COMPARISON_DIR, filename)


def generate_all_speedup_charts() -> None:
    print()
    print(">>> Generowanie wykresów speedup...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    charts = [
        ("Speedup vs Cores (na 1 algorytm)", plot_speedup_vs_cores_per_algorithm),
        ("Speedup vs Cores (porównanie)", plot_speedup_vs_cores_comparison),
        ("Speedup vs Data Size", plot_speedup_vs_data_size),
        ("Speedup Comparison (ranking)", plot_speedup_comparison),
    ]

    for name, function in charts:
        print()
        print(f"--- {name} ---")
        function(df)

    print()
    print(">>> Zakończono generowanie wykresów speedup")