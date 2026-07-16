import multiprocessing as mp
import math

from core.database import get_data_from_db
from core.benchmark import profile_function
from core.menu import print_separator, get_available_cores
from core.config import  ALGORITHMS, DATA_TABLES, DATA_SIZES
from core.results_database import create_results_table, save_benchmark_result

TEST_ALGORITHMS = {
    "1": ALGORITHMS["1"],
    "2": ALGORITHMS["2"]
}
TEST_TABLES = {
    "1": DATA_TABLES["1"],
    "3": DATA_TABLES["3"]
}
TEST_SIZES = {
    "1": DATA_SIZES["2"],
    "2": DATA_SIZES["3"]
}
TEST_CORES = [2, 4]


def run_quick_auto_benchmarks():
    create_results_table()
    available_cores = TEST_CORES

    total_tests = (
        len(TEST_ALGORITHMS)
        * len(TEST_TABLES)
        * len(TEST_SIZES)
        * len(available_cores)
    )

    current_test = 0

    print_separator()
    print("Automatyczne testy")
    print(f"Liczba wszystkich testów: {total_tests}")

    for algorithm in TEST_ALGORITHMS.values():
        for table_data in TEST_TABLES.values():
            table_name = table_data[0]

            for set_size in TEST_SIZES.values():
                print("Wczytane dane")
                print(f"Algorytm: {algorithm['name']}")
                print(f"Tabela: {table_name}")
                print(f"Rozmiar danych: {set_size}")

                data = get_data_from_db(table_name,set_size)

                print(f"Pobrano {len(data)} rekordów\n")

                print("Test sekwencyjny")
                print_separator()

                sequential_stats = profile_function(
                    algorithm["sequential"],
                    data,
                    label=f"{algorithm['name']} - Sequential"
                )

                sequential_result = sequential_stats["result"]

                save_benchmark_result(
                    algorithm=algorithm["name"],
                    mode="Sequential",
                    dataset=table_name,
                    data_size=set_size,
                    cores=1,
                    stats=sequential_stats
                )

                for cores in available_cores:
                    current_test += 1

                    print_separator()
                    print(f"Test {current_test}/{total_tests}")
                    print(f"Liczba rdzeni: {cores}")
                    print("Test równoległy")
                    print_separator()

                    if algorithm["name"] == "Quick Sort":

                        max_depth = int(math.log2(cores))

                        parallel_stats = profile_function(
                            algorithm["parallel"],
                            data,
                            max_depth,
                            label=f"{algorithm['name']} - Parallel",
                            sequential_time=sequential_stats["avg_time"],
                            cores=cores
                        )

                    elif algorithm["name"] == "Merge Sort":
                        max_depth = int(math.log2(cores))

                        parallel_stats = profile_function(
                            algorithm["parallel"],
                            data,
                            max_depth,
                            label=f"{algorithm['name']} - Parallel",
                            sequential_time=sequential_stats["avg_time"],
                            cores=cores
                        )

                    elif algorithm["name"] == "Bucket Sort":
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
                        continue

                    parallel_result = parallel_stats["result"]

                    save_benchmark_result(
                        algorithm=algorithm["name"],
                        mode="Parallel",
                        dataset=table_name,
                        data_size=set_size,
                        cores=cores,
                        stats=parallel_stats
                    )

                    print("Weryfikacja")
                    if sequential_result == parallel_result:
                        print("Sortowanie poprawne")
                    else:
                        print("Błąd: Wyniki sortowania różnią się")

    print_separator()
    print("Wszystkie testy zakończone")
    print_separator()