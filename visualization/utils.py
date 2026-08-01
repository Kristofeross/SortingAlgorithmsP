import matplotlib.pyplot as plt
from pathlib import Path

from visualization.config import (
    CHARTS_DIR,

    EXECUTION_TIME_DIR,
    EXECUTION_TIME_VS_DATA_SIZE_DIR,
    EXECUTION_TIME_VS_CORES_DIR,
    EXECUTION_TIME_SEQUENTIAL_VS_PARALLEL_DIR,
    EXECUTION_TIME_ALGORITHM_COMPARISON_DIR,

    SPEEDUP_DIR,
    SPEEDUP_VS_DATA_SIZE_DIR,
    SPEEDUP_VS_CORES_DIR,
    SPEEDUP_COMPARISON_DIR,

    EFFICIENCY_DIR,
    EFFICIENCY_VS_DATA_SIZE_DIR,
    EFFICIENCY_VS_CORES_DIR,
    EFFICIENCY_COMPARISON_DIR,

    CPU_DIR,
    CPU_VS_DATA_SIZE_DIR,
    CPU_VS_CORES_DIR,
    CPU_COMPARISON_DIR,

    MEMORY_DIR,
    MEMORY_VS_DATA_SIZE_DIR,
    MEMORY_VS_CORES_DIR,
    MEMORY_COMPARISON_DIR,

    DATASETS_DIR,
    DATASETS_IMPACT_DIR,
    DATASETS_SORTEDNESS_DIR,
    HEATMAPS_DIR,
    RANKING_DIR,

    EXPORT_FORMATS,
    FIGURE_SIZE,
    DPI,
    SAVE_BBOX,
)

from visualization.style import apply_plot_style


def ensure_directory(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)


def create_results_directories() -> None:
    ensure_directory(CHARTS_DIR)

    directories = [
        CHARTS_DIR,

        EXECUTION_TIME_DIR,
        EXECUTION_TIME_VS_DATA_SIZE_DIR,
        EXECUTION_TIME_VS_CORES_DIR,
        EXECUTION_TIME_SEQUENTIAL_VS_PARALLEL_DIR,
        EXECUTION_TIME_ALGORITHM_COMPARISON_DIR,

        SPEEDUP_DIR,
        SPEEDUP_VS_DATA_SIZE_DIR,
        SPEEDUP_VS_CORES_DIR,
        SPEEDUP_COMPARISON_DIR,

        EFFICIENCY_DIR,
        EFFICIENCY_VS_DATA_SIZE_DIR,
        EFFICIENCY_VS_CORES_DIR,
        EFFICIENCY_COMPARISON_DIR,

        CPU_DIR,
        CPU_VS_DATA_SIZE_DIR,
        CPU_VS_CORES_DIR,
        CPU_COMPARISON_DIR,

        MEMORY_DIR,
        MEMORY_VS_DATA_SIZE_DIR,
        MEMORY_VS_CORES_DIR,
        MEMORY_COMPARISON_DIR,

        DATASETS_DIR,
        DATASETS_IMPACT_DIR,
        DATASETS_SORTEDNESS_DIR,
        HEATMAPS_DIR,
        RANKING_DIR,
    ]

    for directory in directories:
        ensure_directory(directory)


def create_figure():
    return plt.subplots(figsize=FIGURE_SIZE)


def get_distinct_colors(n: int):
    cmap = plt.get_cmap("viridis")
    return [cmap(i / max(n - 1, 1)) for i in range(n)]


def save_plot(fig, directory: Path, filename: str,) -> None:
    output_directory = Path(directory)
    ensure_directory(output_directory)

    for extension in EXPORT_FORMATS:
        fig.savefig(
            output_directory / f"{filename}.{extension}",
            dpi=DPI,
            bbox_inches=SAVE_BBOX,
        )


def close_plot(fig) -> None:
    plt.close(fig)


def finish_plot(fig, ax, directory: Path, filename: str,) -> None:
    apply_plot_style(ax)
    save_plot(fig=fig, directory=directory, filename=filename,)
    close_plot(fig)