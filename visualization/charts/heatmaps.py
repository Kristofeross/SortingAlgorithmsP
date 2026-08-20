import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from visualization.config import DATASET_LABELS, DEFAULT_DATASET, HEATMAPS_DIR
from visualization.loader import load_all, get_algorithms, get_data_sizes, get_cores, get_datasets
from visualization.utils import ensure_directory, save_plot, close_plot
from visualization.filters import filter_algorithm, filter_dataset
from visualization.style import format_log_axis_plain


def plot_execution_time_heatmap(df, dataset: str = DEFAULT_DATASET):
    print("Generowanie: Execution Time Heatmap")

    algorithms = get_algorithms()
    data_sizes = sorted(get_data_sizes())
    cores_values = sorted(get_cores())

    dataset_df = filter_dataset(df, dataset)

    if dataset_df.empty:
        print(f"  Brak danych dla zbioru '{dataset}', pomijam.")
        return

    dataset_label = DATASET_LABELS.get(dataset, dataset)

    for algorithm in algorithms:
        algorithm_df = filter_algorithm(dataset_df, algorithm)

        if algorithm_df.empty:
            continue

        matrix = np.full((len(data_sizes), len(cores_values)), np.nan)

        for i, size in enumerate(data_sizes):
            for j, cores in enumerate(cores_values):
                cell = algorithm_df[
                    (algorithm_df["data_size"] == size)
                    & (algorithm_df["cores"] == cores)
                ]

                if not cell.empty:
                    matrix[i, j] = cell["avg_time"].mean()

        if np.all(np.isnan(matrix)):
            continue

        fig, ax = plt.subplots(figsize=(10, 7))

        valid_values = matrix[~np.isnan(matrix)]
        norm = LogNorm(vmin=valid_values.min(), vmax=valid_values.max())

        im = ax.imshow(matrix, cmap="YlOrRd", norm=norm, aspect="auto")

        ax.set_xticks(range(len(cores_values)))
        ax.set_xticklabels(cores_values)
        ax.set_yticks(range(len(data_sizes)))
        ax.set_yticklabels([f"{size:,}" for size in data_sizes])

        ax.set_xlabel("Liczba rdzeni")
        ax.set_ylabel("Rozmiar danych")
        ax.set_title(
            f"{algorithm}\n"
            f"Czas wykonania: rozmiar danych x liczba rdzeni\n"
            f"{dataset_label}"
        )

        for i in range(len(data_sizes)):
            for j in range(len(cores_values)):
                value = matrix[i, j]

                if np.isnan(value):
                    continue

                relative = (np.log(value) - np.log(norm.vmin)) / (np.log(norm.vmax) - np.log(norm.vmin))
                text_color = "white" if relative > 0.6 else "black"

                ax.text(
                    j, i, f"{value:.3g}s",
                    ha="center", va="center",
                    color=text_color, fontsize=10,
                )

        cbar = fig.colorbar(im, ax=ax, label="Czas wykonania [s] (skala log)")
        format_log_axis_plain(cbar.ax, axis="y")

        filename = algorithm.lower().replace(" ", "_") + "_heatmap_" + dataset

        ensure_directory(HEATMAPS_DIR)
        save_plot(fig, HEATMAPS_DIR, filename, subfolder=dataset)
        close_plot(fig)


def generate_all_heatmap_charts() -> None:
    print()
    print(">>> Generowanie heatmap...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    for dataset in get_datasets():
        plot_execution_time_heatmap(df, dataset=dataset)

    print()
    print(">>> Zakończono generowanie heatmap")