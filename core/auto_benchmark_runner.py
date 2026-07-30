import math

from core.database import get_data_from_db
from core.benchmark import profile_function
from core.menu import print_separator
from core.config import ALGORITHMS, DATA_TABLES, DATA_SIZES
from core.hardware import get_system_info, get_available_cores
from core.results_database import create_results_table, create_system_info_table, save_system_info, save_benchmark_result


USE_LOGICAL_CORES = False


def run_auto_benchmarks():
    create_results_table()
    create_system_info_table()
    save_system_info(get_system_info())

    available_cores, physical, logical = get_available_cores(use_logical=USE_LOGICAL_CORES)

    total_tests = (
        len(ALGORITHMS)
        * len(DATA_TABLES)
        * len(DATA_SIZES)
        * (1 + len(available_cores))
    )

    current_test = 0

    print_separator()
    print("Automatyczne testy")
    print(f"Liczba wszystkich testów: {total_tests}")

    for algorithm in ALGORITHMS.values():
        for table_data in DATA_TABLES.values():

            table_name = table_data[0]

            for data_size in DATA_SIZES.values():
                print_separator()
                print("Wczytane dane")
                print_separator()

                print(f"Algorytm: {algorithm['name']}")
                print(f"Tabela: {table_name}")
                print(f"Rozmiar danych: {data_size}")

                data = get_data_from_db(table_name, data_size)

                print(f"Pobrano {len(data)} rekordów")

                # Sequential benchmark
                current_test += 1

                print_separator()
                print(f"Test {current_test}/{total_tests}")
                print("Test sekwencyjny")
                print_separator()

                sequential_stats = profile_function(
                    algorithm["sequential"],
                    data,
                    label=f"{algorithm['name']} - Sequential"
                )

                save_benchmark_result(
                    algorithm=algorithm["name"],
                    mode="Sequential",
                    dataset=table_name,
                    data_size=data_size,
                    cores=1,
                    stats=sequential_stats
                )

                if sequential_stats["status"] != "OK":
                    print(f"Benchmark sekwencyjny zakończył się: {sequential_stats['status']} | Pomijanie tej konfiguracji")
                    continue

                if sequential_stats["correctness"] != "CORRECT":
                    print(f"Błąd: {sequential_stats['error_message']}")
                    continue


                # Parallel benchmark
                for cores in available_cores:
                    current_test += 1

                    print_separator()
                    print(f"Test {current_test}/{total_tests}")
                    print(f"Liczba rdzeni: {cores}")
                    print("Test równoległy")
                    print_separator()

                    if algorithm["name"] in ("Quick Sort", "Merge Sort"):
                        max_depth = int(math.log2(cores))

                        parallel_stats = profile_function(
                            algorithm["parallel"],
                            data,
                            max_depth,
                            label=f"{algorithm['name']} - Parallel",
                            sequential_time=sequential_stats["avg_time"],
                            cores=cores
                        )

                    elif algorithm["name"] in ("Bucket Sort", "Sample Sort"):
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
                        continue


                    save_benchmark_result(
                        algorithm=algorithm["name"],
                        mode="Parallel",
                        dataset=table_name,
                        data_size=data_size,
                        cores=cores,
                        stats=parallel_stats
                    )

                    if parallel_stats["status"] != "OK":
                        print(f"Benchmark zakończony błędem: {parallel_stats['error_message']}")
                        continue

                    if parallel_stats["correctness"] != "CORRECT":
                        print(f"Błąd: {parallel_stats['error_message']}")
                        continue


    print_separator()
    print("Wszystkie testy zakończone")
    print_separator()