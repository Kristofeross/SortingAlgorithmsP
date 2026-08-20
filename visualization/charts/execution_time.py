from visualization.config import (
    ALGORITHM_COLORS, ALGORITHM_MARKERS, DATASET_LABELS,
    DEFAULT_DATASET,
    EXECUTION_TIME_VS_DATA_SIZE_DIR, EXECUTION_TIME_VS_CORES_DIR,
    EXECUTION_TIME_SEQUENTIAL_VS_PARALLEL_DIR, EXECUTION_TIME_ALGORITHM_COMPARISON_DIR,
)
from visualization.loader import load_all, get_algorithms, get_data_sizes, get_datasets
from visualization.utils import create_figure, finish_plot
from visualization.style import use_log_scale_x, use_log_scale_y, set_clean_ticks, format_log_axis_plain
from visualization.filters import filter_algorithm, filter_dataset, filter_parallel, filter_sequential, sort_by_data_size


def plot_execution_time_vs_data_size(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Execution Time vs Data Size")

    algorithms = get_algorithms()

    dataset_df = filter_dataset(df, dataset)
    dataset_df = filter_parallel(dataset_df)

    if dataset_df.empty:
        print(f"  Brak danych dla zbioru '{dataset}', pomijam.")
        return

    fig, ax = create_figure()

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)
        algorithm_df = sort_by_data_size(algorithm_df)

        if algorithm_df.empty:
            continue

        ax.errorbar(
            algorithm_df["data_size"],
            algorithm_df["avg_time"],
            yerr=algorithm_df.get("std_time"),

            label=algorithm,
            color=ALGORITHM_COLORS.get(algorithm),
            marker=ALGORITHM_MARKERS.get(algorithm),
            capsize=3,
        )

    use_log_scale_x(ax)
    use_log_scale_y(ax)
    set_clean_ticks(ax, dataset_df["data_size"].unique())
    format_log_axis_plain(ax)

    dataset_label = DATASET_LABELS.get(dataset, dataset)
    ax.set_title(f"Czas wykonania w zależności od rozmiaru danych\n{dataset_label}")
    ax.set_xlabel("Rozmiar danych")
    ax.set_ylabel("Czas wykonania [s]")

    filename = "execution_time_vs_data_size_" + dataset

    finish_plot(fig, ax, EXECUTION_TIME_VS_DATA_SIZE_DIR, filename, subfolder=dataset)


def plot_execution_time_vs_cores(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Execution Time vs Cores")

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

    fig, ax = create_figure()

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)
        algorithm_df = algorithm_df.sort_values("cores")

        if algorithm_df.empty:
            continue

        ax.errorbar(
            algorithm_df["cores"],
            algorithm_df["avg_time"],
            yerr=algorithm_df.get("std_time"),

            label=algorithm,
            color=ALGORITHM_COLORS.get(algorithm),
            marker=ALGORITHM_MARKERS.get(algorithm),
            capsize=3,
        )

    use_log_scale_x(ax)
    set_clean_ticks(ax, dataset_df["cores"].unique())

    dataset_label = DATASET_LABELS.get(dataset, dataset)
    ax.set_title(
        f"Czas wykonania w zależności od liczby rdzeni\n"
        f"{dataset_label}, {max_size:,} elementów"
    )
    ax.set_xlabel("Liczba rdzeni")
    ax.set_ylabel("Czas wykonania [s]")

    filename = "execution_time_vs_cores_" + dataset

    finish_plot(fig, ax, EXECUTION_TIME_VS_CORES_DIR, filename, subfolder=dataset)


def plot_sequential_vs_parallel(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Sequential vs Parallel")

    algorithms = get_algorithms()
    dataset_df = filter_dataset(df, dataset)

    if dataset_df.empty:
        print(f"  Brak danych dla zbioru '{dataset}', pomijam.")
        return

    dataset_label = DATASET_LABELS.get(dataset, dataset)

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)

        if algorithm_df.empty:
            continue

        sequential_df = filter_sequential(algorithm_df)
        sequential_df = sort_by_data_size(sequential_df)

        parallel_df = filter_parallel(algorithm_df)

        if parallel_df.empty or sequential_df.empty:
            continue

        max_cores = parallel_df["cores"].max()
        parallel_df = parallel_df[parallel_df["cores"] == max_cores]
        parallel_df = sort_by_data_size(parallel_df)

        fig, ax = create_figure()

        ax.plot(
            sequential_df["data_size"],
            sequential_df["avg_time"],
            label="Sekwencyjnie",
            marker="o",
        )

        ax.plot(
            parallel_df["data_size"],
            parallel_df["avg_time"],
            label=f"Równolegle ({max_cores} rdzeni)",
            marker="s",
        )

        use_log_scale_x(ax)
        use_log_scale_y(ax)
        set_clean_ticks(ax, sequential_df["data_size"].unique())
        format_log_axis_plain(ax)

        ax.set_title(
            f"{algorithm}\n"
            f"Sekwencyjnie vs równolegle\n"
            f"{dataset_label}"
        )
        ax.set_xlabel("Rozmiar danych")
        ax.set_ylabel("Czas wykonania [s]")

        filename = algorithm.lower().replace(" ", "_") + "_sequential_vs_parallel_" + dataset

        finish_plot(fig, ax, EXECUTION_TIME_SEQUENTIAL_VS_PARALLEL_DIR, filename, subfolder=dataset)


def plot_algorithm_comparison(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Algorithm Comparison")

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
            ["avg_time"]
            .mean()
            .sort_values("avg_time")
        )

        if comparison_df.empty:
            continue

        fig, ax = create_figure()

        colors = [
            ALGORITHM_COLORS.get(algorithm)
            for algorithm in comparison_df["algorithm"]
        ]

        ax.bar(
            comparison_df["algorithm"],
            comparison_df["avg_time"],
            color=colors,
        )

        ax.set_title(
            f"Porównanie algorytmów (ranking)\n"
            f"{dataset_label}, {size:,} elementów, {max_cores} rdzeni"
        )
        ax.set_xlabel("Algorytm")
        ax.set_ylabel("Czas wykonania [s]")
        ax.tick_params(axis="x", rotation=20)

        filename = f"algorithm_comparison_{dataset}_{size}"

        finish_plot(fig, ax, EXECUTION_TIME_ALGORITHM_COMPARISON_DIR, filename, subfolder=dataset)


def generate_all_execution_time_charts() -> None:
    print()
    print(">>> Generowanie wykresów czasu wykonania...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    datasets = get_datasets()

    charts = [
        ("Execution Time vs Data Size", plot_execution_time_vs_data_size),
        ("Execution Time vs Cores", plot_execution_time_vs_cores),
        ("Sequential vs Parallel", plot_sequential_vs_parallel),
        ("Algorithm Comparison", plot_algorithm_comparison),
    ]

    for name, function in charts:
        print()
        print(f"--- {name} ---")

        for dataset in datasets:
            function(df, dataset=dataset)

    print()
    print(">>> Zakończono generowanie wykresów czasu wykonania")