from visualization.utils import create_results_directories
from visualization.charts.execution_time import generate_all_execution_time_charts
from visualization.charts.datasets import generate_all_dataset_charts
from visualization.charts.speedup import generate_all_speedup_charts
from visualization.charts.efficiency import generate_all_efficiency_charts
from visualization.charts.cpu import generate_all_cpu_charts
from visualization.charts.memory import generate_all_memory_charts
from visualization.charts.heatmaps import generate_all_heatmap_charts
from visualization.charts.ranking import generate_all_ranking_charts
from visualization.analysis.decision_table import generate_decision_table
from visualization.analysis.complexity import generate_complexity_analysis
from visualization.tables.summary import generate_summary_table
from visualization.tables.scalability import generate_scalability_tables
from visualization.tables.resources import generate_resources_table
from visualization.tables.datasets import generate_dataset_impact_table

def generate_all() -> None:
    print("=" * 70)
    print("Generowanie wykresów")

    create_results_directories()

    # # Execution time
    # generate_all_execution_time_charts()
    #
    # # Influence of input data
    # generate_all_dataset_charts()
    #
    # # Speedup
    # generate_all_speedup_charts()
    #
    # # EFFICIENCY
    # generate_all_efficiency_charts()
    #
    # # CPU
    # generate_all_cpu_charts()
    #
    # # Memory
    # generate_all_memory_charts()
    #
    # # Heatmaps
    # generate_all_heatmap_charts()
    #
    # # Overall ranking
    # generate_all_ranking_charts()
    #
    # # Decision table and complexity analysis
    # generate_decision_table()
    # generate_complexity_analysis()

    # Tables
    generate_summary_table()
    generate_scalability_tables()
    generate_resources_table()
    generate_dataset_impact_table()

    print()
    print("================   Zakończeno generowanie wykresów   ================")


if __name__ == "__main__":
    generate_all()