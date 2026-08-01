import numpy as np
import matplotlib.pyplot as plt

from visualization.config import DATASET_LABELS, DEFAULT_DATASET, DEFAULT_DATA_SIZE, DEFAULT_CORES, RANKING_DIR
from visualization.loader import load_all, get_algorithms, get_data_sizes
from visualization.utils import ensure_directory, save_plot, close_plot
from visualization.filters import filter_dataset, filter_parallel


# Metrics included in the aggregate ranking
RANKING_METRICS = [
    ("avg_time", "Czas wykonania [s]", False),
    ("speedup", "Speedup", True),
    ("efficiency", "Efficiency", True),
    ("avg_cpu", "CPU [%]", True),
    ("avg_mem", "RAM [MB]", False),
]


def resolve_data_size(df, preferred: int):
    available = get_data_sizes()

    if not available:
        return None

    if preferred in available:
        return preferred

    return max(available)


# Chart: aggregate heatmap ranking
def plot_multi_metric_ranking(df,
    dataset: str = DEFAULT_DATASET,
    data_size: int = DEFAULT_DATA_SIZE,
    cores: int = DEFAULT_CORES,
):
    print("Generowanie: Multi-Metric Ranking")

    resolved_size = resolve_data_size(df, data_size)

    if resolved_size is None:
        print("  Brak danych, pomijam.")
        return

    algorithms = get_algorithms()

    subset_df = filter_dataset(df, dataset)
    subset_df = subset_df[subset_df["data_size"] == resolved_size]
    subset_df = filter_parallel(subset_df)
    subset_df = subset_df[subset_df["cores"] == cores]

    if subset_df.empty:
        print(f"  Brak danych dla rozmiaru={resolved_size}, rdzeni={cores}, pomijam.")
        return

    raw = np.full((len(algorithms), len(RANKING_METRICS)), np.nan)

    for i, algorithm in enumerate(algorithms):
        algo_df = subset_df[subset_df["algorithm"] == algorithm]

        if algo_df.empty:
            continue

        for j, (column, _, _) in enumerate(RANKING_METRICS):
            if column in algo_df.columns:
                raw[i, j] = algo_df[column].mean()

    if np.all(np.isnan(raw)):
        print("  Brak wartości do wyliczenia rankingu, pomijam.")
        return

    normalized = np.full_like(raw, np.nan)

    for j, (_, _, higher_is_better) in enumerate(RANKING_METRICS):
        column_values = raw[:, j]
        valid = ~np.isnan(column_values)

        if valid.sum() == 0:
            continue

        col_min = column_values[valid].min()
        col_max = column_values[valid].max()

        if col_max == col_min:
            normalized[valid, j] = 1.0
            continue

        scaled = (column_values - col_min) / (col_max - col_min)

        if not higher_is_better:
            scaled = 1 - scaled

        normalized[:, j] = scaled

    dataset_label = DATASET_LABELS.get(dataset, dataset)
    metric_labels = [label for _, label, _ in RANKING_METRICS]

    fig, ax = plt.subplots(figsize=(11, 6))

    im = ax.imshow(normalized, cmap="RdYlGn", vmin=0, vmax=1, aspect="auto")

    ax.set_xticks(range(len(metric_labels)))
    ax.set_xticklabels(metric_labels)
    ax.set_yticks(range(len(algorithms)))
    ax.set_yticklabels(algorithms)

    ax.set_title(
        f"Zbiorczy ranking algorytmów wg wszystkich metryk\n"
        f"{dataset_label}, {resolved_size:,} elementów, {cores} rdzeni"
    )

    for i in range(len(algorithms)):
        for j, (_, _, _) in enumerate(RANKING_METRICS):
            value = raw[i, j]
            norm_value = normalized[i, j]

            if np.isnan(value):
                continue

            text_color = "white" if (norm_value < 0.3 or norm_value > 0.75) else "black"

            ax.text(
                j, i, f"{value:.3g}",
                ha="center", va="center",
                color=text_color, fontsize=11,
            )

    fig.colorbar(im, ax=ax, label="Wynik znormalizowany (0 = najgorszy, 1 = najlepszy)")

    filename = f"multi_metric_ranking_{dataset}_{resolved_size}_{cores}cores"

    ensure_directory(RANKING_DIR)
    save_plot(fig, RANKING_DIR, filename)
    close_plot(fig)


def generate_all_ranking_charts() -> None:
    print()
    print(">>> Generowanie zbiorczego rankingu...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    plot_multi_metric_ranking(df)

    print()
    print(">>> Zakończono generowanie zbiorczego rankingu")