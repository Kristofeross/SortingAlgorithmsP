from pathlib import Path

# Paths
VISUALIZATION_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = VISUALIZATION_DIR.parent

RESULTS_DIR = PROJECT_ROOT / "results"
CHARTS_DIR = RESULTS_DIR / "charts"
TABLES_DIR = RESULTS_DIR / "tables"
REPORTS_DIR = RESULTS_DIR / "reports"

# Chart catalogs
EXECUTION_TIME_DIR = CHARTS_DIR / "execution_time"
EXECUTION_TIME_VS_DATA_SIZE_DIR = EXECUTION_TIME_DIR / "vs_data_size"
EXECUTION_TIME_VS_CORES_DIR = EXECUTION_TIME_DIR / "vs_cores"
EXECUTION_TIME_SEQUENTIAL_VS_PARALLEL_DIR = EXECUTION_TIME_DIR / "sequential_vs_parallel"
EXECUTION_TIME_ALGORITHM_COMPARISON_DIR = EXECUTION_TIME_DIR / "algorithm_comparison"

SPEEDUP_DIR = CHARTS_DIR / "speedup"
SPEEDUP_VS_DATA_SIZE_DIR = SPEEDUP_DIR / "vs_data_size"
SPEEDUP_VS_CORES_DIR = SPEEDUP_DIR / "vs_cores"
SPEEDUP_COMPARISON_DIR = SPEEDUP_DIR / "comparison"

EFFICIENCY_DIR = CHARTS_DIR / "efficiency"
EFFICIENCY_VS_DATA_SIZE_DIR = EFFICIENCY_DIR / "vs_data_size"
EFFICIENCY_VS_CORES_DIR = EFFICIENCY_DIR / "vs_cores"
EFFICIENCY_COMPARISON_DIR = EFFICIENCY_DIR / "comparison"

CPU_DIR = CHARTS_DIR / "cpu"
CPU_VS_DATA_SIZE_DIR = CPU_DIR / "vs_data_size"
CPU_VS_CORES_DIR = CPU_DIR / "vs_cores"
CPU_COMPARISON_DIR = CPU_DIR / "comparison"

MEMORY_DIR = CHARTS_DIR / "memory"
MEMORY_VS_DATA_SIZE_DIR = MEMORY_DIR / "vs_data_size"
MEMORY_VS_CORES_DIR = MEMORY_DIR / "vs_cores"
MEMORY_COMPARISON_DIR = MEMORY_DIR / "comparison"

DATASETS_DIR = CHARTS_DIR / "datasets"
DATASETS_IMPACT_DIR = DATASETS_DIR / "impact"
DATASETS_SORTEDNESS_DIR = DATASETS_DIR / "sortedness"

HEATMAPS_DIR = CHARTS_DIR / "heatmaps"
RANKING_DIR = CHARTS_DIR / "ranking"

# Chart parameters
FIGURE_WIDTH = 12
FIGURE_HEIGHT = 7
FIGURE_SIZE = (FIGURE_WIDTH, FIGURE_HEIGHT)

DPI = 300
IMAGE_FORMAT = "png"
SAVE_BBOX = "tight"

LINE_WIDTH = 2.5
MARKER_SIZE = 8

GRID_ALPHA = 0.30

# Fonts
TITLE_SIZE = 18
LABEL_SIZE = 14
TICK_SIZE = 12
LEGEND_SIZE = 11

# Dataset Labels
DATASET_LABELS = {
    "random_int": "Losowe liczby całkowite",
    "random_float": "Losowe liczby zmiennoprzecinkowe",

    "duplicates_int": "Duplikaty (int)",
    "duplicates_float": "Duplikaty (float)",

    "part_sorted20_int": "20% posortowanych (int)",
    "part_sorted20_float": "20% posortowanych (float)",

    "part_sorted40_int": "40% posortowanych (int)",
    "part_sorted40_float": "40% posortowanych (float)",

    "part_sorted60_int": "60% posortowanych (int)",
    "part_sorted60_float": "60% posortowanych (float)",

    "part_sorted80_int": "80% posortowanych (int)",
    "part_sorted80_float": "80% posortowanych (float)",
}

# Datasets used for the "Data Type Influence" chart
DATASET_IMPACT_SETS = [
    "random_int",
    "random_float",
    "duplicates_int",
    "duplicates_float",
]

# Sets used for the "effect of sorting degree" graph
SORTEDNESS_SETS = {
    "int": [
        (0, "random_int"),
        (20, "part_sorted20_int"),
        (40, "part_sorted40_int"),
        (60, "part_sorted60_int"),
        (80, "part_sorted80_int"),
    ],
    "float": [
        (0, "random_float"),
        (20, "part_sorted20_float"),
        (40, "part_sorted40_float"),
        (60, "part_sorted60_float"),
        (80, "part_sorted80_float"),
    ],
}

# Colors of algorithms
ALGORITHM_COLORS = {
    "Quick Sort": "tab:blue",
    "Merge Sort": "tab:orange",
    "Bucket Sort": "tab:green",
    "Sample Sort": "tab:red",
}

# Algorithm markers
ALGORITHM_MARKERS = {
    "Quick Sort": "o",
    "Merge Sort": "s",
    "Bucket Sort": "^",
    "Sample Sort": "D",
}

# Metric labels
METRIC_LABELS = {
    "avg_time": "Średni czas [s]",
    "median_time": "Mediana [s]",
    "std_time": "Odchylenie standardowe [s]",
    "avg_cpu": "Średnie użycie CPU [%]",
    "avg_mem": "Średnie użycie RAM [MB]",
    "max_mem": "Maksymalne użycie RAM [MB]",
    "speedup": "Speedup",
    "efficiency": "Efficiency",
}

# Default chart parameters
DEFAULT_DATASET = "random_int"
DEFAULT_DATA_SIZE = 1_000_000
DEFAULT_CORES = 8
DEFAULT_MODE = "Parallel"

# Export format
EXPORT_FORMATS = [
    "png",
]