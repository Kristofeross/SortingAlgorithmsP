import math

from core.database import get_data_from_db
from core.benchmark import profile_function
from core.menu import print_separator
from core.config import  ALGORITHMS, DATA_TABLES, DATA_SIZES
from core.hardware import get_system_info
from core.results_database import create_system_info_table, save_system_info, create_results_table, save_benchmark_result

TEST_ALGORITHMS = {
    # "1": ALGORITHMS["1"], # Quick Sort
    # "2": ALGORITHMS["2"], # Merge Sort
    # "3": ALGORITHMS["3"], # Bucket Sort
    "4": ALGORITHMS["4"]  # Sample Sort
}
TEST_TABLES = {
    "1": DATA_TABLES["1"], # Losowe liczby całkowite
    # "2": DATA_TABLES["2"], # Losowe liczby zmiennoprzecinkowe
    # "3": DATA_TABLES["3"], # Całkowite liczby z duplikatami
    # "4": DATA_TABLES["4"], # Zmienne liczby z duplikatami
    # "5": DATA_TABLES["5"], # 20% posortowanych liczb całkowitych
    # "6": DATA_TABLES["6"], # 20% posortowanych liczb zmiennoprzecinkowych
    # "7": DATA_TABLES["7"], # 40% posortowanych liczb całkowitych
    # "8": DATA_TABLES["8"], # 40% posortowanych liczb zmiennoprzecinkowych
    # "9": DATA_TABLES["9"], # 60% posortowanych liczb całkowitych
    # "10": DATA_TABLES["10"], # 60% posortowanych liczb zmiennoprzecinkowych
    # "11": DATA_TABLES["11"], # 80% posortowanych liczb całkowitych
    # "12": DATA_TABLES["12"]  # 80% posortowanych liczb zmiennoprzecinkowych
}
TEST_SIZES = {
    "1": DATA_SIZES["1"], # 1000
    "2": DATA_SIZES["2"], # 10000
    "3": DATA_SIZES["3"], # 100000
    "4": DATA_SIZES["4"]  # 1000000
}
TEST_CORES = [2, 4, 8]


def run_quick_auto_benchmarks():
    create_results_table()
    create_system_info_table()
    save_system_info(get_system_info())

    available_cores = TEST_CORES

    total_tests = (
        len(TEST_ALGORITHMS)
        * len(TEST_TABLES)
        * len(TEST_SIZES)
        * (1+ len(available_cores))
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

                data = get_data_from_db(table_name, set_size)

                print(f"Pobrano {len(data)} rekordów\n")

                # Sequential benchmark
                current_test += 1

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
                    data_size=set_size,
                    cores=1,
                    stats=sequential_stats
                )

                if sequential_stats["status"] != "OK":
                    print("Pomijanie testów równoległych z powodu błędu sekwencyjnego.")
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
                        data_size=set_size,
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