import multiprocessing as mp

from core.database import get_data_from_db
from core.benchmark import profile_function
from core.menu import print_separator, choose_algorithm, choose_table, choose_size, choose_cores, get_available_cores
from core.results_database import create_results_table, save_benchmark_result


def run_manual_benchmarks():
    create_results_table()

    # Choice of configurations
    algorithm = choose_algorithm()
    table_name = choose_table()
    set_size = choose_size()
    cores = choose_cores()

    # Download data
    print_separator()
    print("Wczytane dane")
    print_separator()

    print(f"Tabela: {table_name}")
    print(f"Rozmiar danych: {set_size}")
    print(f"Liczba rdzeni: {cores}")

    data = get_data_from_db(table_name, set_size)
    print(f"Pobrano {len(data)} rekordów")

    # Sequential test
    print_separator()
    print("Test sekwencyjny")
    print_separator()

    sequential_stats = profile_function(
        algorithm["sequential"],
        data,
        label=f"{algorithm['name']} - Sequential"
    )

    sequential_result = sequential_stats["result"]
    save_benchmark_result(algorithm=algorithm["name"], mode="Sequential", dataset=table_name, data_size=set_size,cores=1, stats=sequential_stats)

    # Parallel test
    print_separator()
    print("Test równoległy")
    print_separator()

    if algorithm["name"] == "Parallel QuickSort":
        import math
        max_depth = int(math.log2(cores))

        parallel_stats = profile_function(
            algorithm["parallel"],
            data,
            max_depth,
            label=f"{algorithm['name']} - Parallel",
            sequential_time=sequential_stats["avg_time"],
            cores=cores
        )

    elif algorithm["name"] == "Parallel MergeSort":
        import math
        max_depth = int(math.log2(cores))

        parallel_stats = profile_function(
            algorithm["parallel"],
            data,
            max_depth,
            label=f"{algorithm['name']} - Parallel",
            sequential_time=sequential_stats["avg_time"],
            cores=cores
        )

    elif algorithm["name"] == "Parallel BucketSort":
        parallel_stats = profile_function(
            algorithm["parallel"],
            data,
            cores,
            label=f"{algorithm['name']} - Parallel",
            sequential_time=sequential_stats["avg_time"],
            cores=cores
        )

    elif algorithm["name"] == "Sample Sort":
        parallel_stats = profile_function(
            algorithm["parallel"],
            data,
            cores,
            label=f"{algorithm['name']} - Parallel",
            sequential_time=sequential_stats["avg_time"],
            cores=cores
        )

    else:
        print("Nieobsługiwany algorytm")
        return

    parallel_result = parallel_stats["result"]
    save_benchmark_result( algorithm=algorithm["name"], mode="Parallel", dataset=table_name, data_size=set_size, cores=cores, stats=parallel_stats )

    # Validation
    print("Weryfikacja")
    if sequential_result == parallel_result:
        print("Sortowanie poprawne")
    else:
        print("Błąd: Wyniki sortowania różnią się")


