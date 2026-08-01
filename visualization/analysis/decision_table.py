import matplotlib.pyplot as plt

from visualization.config import ALGORITHM_COLORS, DATASET_LABELS, DECISION_TABLE_DIR
from visualization.loader import load_all, get_algorithms, get_datasets, get_data_sizes
from visualization.utils import ensure_directory
from visualization.filters import filter_dataset, filter_parallel


def build_decision_table(df) -> "pd.DataFrame":
    import pandas as pd

    datasets = get_datasets()
    data_sizes = sorted(get_data_sizes())

    rows = []

    for dataset in datasets:
        dataset_df = filter_dataset(df, dataset)

        for size in data_sizes:
            size_df = dataset_df[dataset_df["data_size"] == size]
            size_df = filter_parallel(size_df)

            if size_df.empty:
                continue

            max_cores = size_df["cores"].max()
            size_df = size_df[size_df["cores"] == max_cores]

            ranking = (
                size_df
                .groupby("algorithm", as_index=False)
                ["avg_time"]
                .mean()
                .sort_values("avg_time")
                .reset_index(drop=True)
            )

            if ranking.empty:
                continue

            winner = ranking.iloc[0]
            runner_up = ranking.iloc[1] if len(ranking) > 1 else None

            if runner_up is not None and winner["avg_time"] > 0:
                advantage_pct = (runner_up["avg_time"] - winner["avg_time"]) / winner["avg_time"] * 100
            else:
                advantage_pct = None

            rows.append({
                "dataset": dataset,
                "dataset_label": DATASET_LABELS.get(dataset, dataset),
                "data_size": size,
                "cores": max_cores,
                "winner": winner["algorithm"],
                "winner_time": winner["avg_time"],
                "runner_up": runner_up["algorithm"] if runner_up is not None else None,
                "runner_up_time": runner_up["avg_time"] if runner_up is not None else None,
                "advantage_pct": advantage_pct,
            })

    return pd.DataFrame(rows)


def export_decision_table_csv(decision_df) -> None:
    ensure_directory(DECISION_TABLE_DIR)
    path = DECISION_TABLE_DIR / "decision_table_detailed.csv"
    decision_df.to_csv(path, index=False, float_format="%.6f")


def export_decision_table_pivot(decision_df) -> None:
    pivot = decision_df.pivot(index="dataset_label", columns="data_size", values="winner")

    ensure_directory(DECISION_TABLE_DIR)
    path = DECISION_TABLE_DIR / "decision_table_pivot.csv"
    pivot.to_csv(path)

    return pivot


def plot_decision_table(pivot) -> None:
    algorithms = get_algorithms()
    algorithm_to_index = {algo: i for i, algo in enumerate(algorithms)}

    n_rows, n_cols = pivot.shape

    fig, ax = plt.subplots(figsize=(2 + n_cols * 1.8, 1 + n_rows * 0.6))
    ax.axis("off")

    cell_colors = [
        [ALGORITHM_COLORS.get(pivot.iloc[i, j], "white") for j in range(n_cols)]
        for i in range(n_rows)
    ]

    cell_text = [
        [str(pivot.iloc[i, j]) for j in range(n_cols)]
        for i in range(n_rows)
    ]

    table = ax.table(
        cellText=cell_text,
        cellColours=cell_colors,
        rowLabels=pivot.index,
        colLabels=[f"{col:,}".replace(",", " ") for col in pivot.columns],
        cellLoc="center",
        loc="center",
    )

    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2.2)

    for (row, col), cell in table.get_celld().items():
        if row > 0 and col >= 0:
            cell.get_text().set_color("white")
            cell.get_text().set_weight("bold")

    ax.set_title(
        "Tabela decyzyjna - najszybszy algorytm\n"
        "wg zbioru danych i rozmiaru danych (maks. liczba rdzeni)",
        fontsize=13,
        pad=20,
    )

    path = DECISION_TABLE_DIR / "decision_table.png"
    ensure_directory(DECISION_TABLE_DIR)
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def generate_decision_table() -> None:
    print()
    print(">>> Generowanie tabeli decyzyjnej...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    decision_df = build_decision_table(df)

    if decision_df.empty:
        print("Brak danych do zbudowania tabeli decyzyjnej.")
        return

    export_decision_table_csv(decision_df)
    pivot = export_decision_table_pivot(decision_df)
    plot_decision_table(pivot)

    print(">>> Zakończono generowanie tabeli decyzyjnej")