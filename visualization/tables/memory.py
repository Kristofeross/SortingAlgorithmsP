from visualization.config import (
    MEMORY_VS_CORES_DIR,
    DEFAULT_DATASET,
    DEFAULT_DATA_SIZE,
    ALGORITHM_COLORS,
    ALGORITHM_MARKERS,
)
from visualization.loader import load_all, get_algorithms
from visualization.filters import filter_dataset, filter_data_size, filter_parallel, filter_algorithm
from visualization.utils import create_figure, finish_plot


def generate_memory_max_vs_cores_chart(
    dataset: str = DEFAULT_DATASET,
    data_size: int = DEFAULT_DATA_SIZE,
) -> None:
    print()
    print(">>> Generowanie wykresu maksymalnego użycia RAM vs liczba jednostek...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    filtered = filter_dataset(df, dataset)
    filtered = filter_data_size(filtered, data_size)
    filtered = filter_parallel(filtered)

    if filtered.empty:
        print("Brak danych do wygenerowania wykresu RAM.")
        return

    fig, ax = create_figure()

    for algorithm in get_algorithms():
        algo_df = filter_algorithm(filtered, algorithm).sort_values("cores")

        if algo_df.empty:
            continue

        ax.plot(
            algo_df["cores"],
            algo_df["max_mem"],
            marker=ALGORITHM_MARKERS.get(algorithm),
            color=ALGORITHM_COLORS.get(algorithm),
            label=algorithm,
        )

    ax.set_title(
        "Maksymalne wykorzystanie pamięci RAM\n"
        f"{dataset}, n = {data_size:,}".replace(",", " ")
    )
    ax.set_xlabel("Liczba jednostek wykonawczych")
    ax.set_ylabel("Maksymalne użycie RAM [MB]")

    ax.set_xticks( sorted(filtered["cores"].unique()) )

    filename = ( f"memory_max_vs_cores_{dataset}_{data_size}" )

    finish_plot(fig=fig, ax=ax, directory=MEMORY_VS_CORES_DIR, filename=filename)

    print(
        f">>> Zapisano wykres: "
        f"{MEMORY_VS_CORES_DIR / (filename + '.png')}"
    )


def generate_memory_avg_vs_cores_chart(
    dataset: str = DEFAULT_DATASET,
    data_size: int = DEFAULT_DATA_SIZE,
) -> None:
    print()
    print(">>> Generowanie wykresu średniego użycia RAM vs liczba jednostek...")

    df = load_all()

    if df.empty:
        print("Brak danych w bazie.")
        return

    filtered = filter_dataset(df, dataset)
    filtered = filter_data_size(filtered, data_size)
    filtered = filter_parallel(filtered)

    if filtered.empty:
        print("Brak danych do wygenerowania wykresu RAM.")
        return

    fig, ax = create_figure()

    for algorithm in get_algorithms():
        algo_df = filter_algorithm(filtered, algorithm).sort_values("cores")

        if algo_df.empty:
            continue

        ax.plot(
            algo_df["cores"],
            algo_df["avg_mem"],
            marker=ALGORITHM_MARKERS.get(algorithm),
            color=ALGORITHM_COLORS.get(algorithm),
            label=algorithm,
        )

    ax.set_title(
        "Średnie wykorzystanie pamięci RAM\n"
        f"{dataset}, n = {data_size:,}".replace(",", " ")
    )
    ax.set_xlabel("Liczba jednostek wykonawczych")
    ax.set_ylabel("Średnie użycie RAM [MB]")

    ax.set_xticks( sorted(filtered["cores"].unique()) )

    filename = ( f"memory_avg_vs_cores_{dataset}_{data_size}" )

    finish_plot(fig=fig, ax=ax, directory=MEMORY_VS_CORES_DIR, filename=filename)

    print(
        f">>> Zapisano wykres: "
        f"{MEMORY_VS_CORES_DIR / (filename + '.png')}"
    )