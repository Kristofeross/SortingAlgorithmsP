import multiprocessing as mp

from algorithms.pqv2 import parallel_quicksort
from algorithms.pmsv1 import parallel_merge_sort
from algorithms.pbsv1 import parallel_bucket_sort

from algorithms.temp_support import (
    get_data_from_db,
    quicksort,
    profile_function
)

from algorithms.pmsv1 import merge_sort
from algorithms.pbsv1 import bucket_sort

# ==========================================
# Stałe
# ==========================================

# Dostępne algorytmy
ALGORITHMS = {
    "1": {
        "name": "Parallel QuickSort",
        "sequential": quicksort,
        "parallel": parallel_quicksort
    },
    "2": {
        "name": "Parallel MergeSort",
        "sequential": merge_sort,
        "parallel": parallel_merge_sort
    },
    "3": {
        "name": "Parallel BucketSort",
        "sequential": bucket_sort,
        "parallel": parallel_bucket_sort
    },
    # "4": {
    #     "name": "Sample Sort",
    #     "sequential": ,
    #     "parallel":
    # },
    # "5": {
    #     "name": "Odd-Even Transposition Sort",
    #     "sequential": ,
    #     "parallel":
    # }
}

# Dostępne tabele danych
DATA_TABLES = {
    "1": ("random_int", "Losowe liczby całkowite"),
    "2": ("random_float", "Losowe liczby zmiennoprzecinkowe"),
    "3": ("duplicates_int", "Całkowite liczby z duplikatami"),
    "4": ("duplicates_float", "Zmienne liczby z duplikatami"),
    "5": ("part_sorted_int", "Częściowo posortowane liczby całkowite"),
    "6": ("part_sorted_float", "Częściowo posortowane liczby zmiennoprzecinkowe")
}

# Rozmiary danych
DATA_SIZES = {
    "1": 1000,
    "2": 10000,
    "3": 100000,
    "4": 1000000
}

# ==========================================
# Funkcje pomocnicze
# ==========================================
def print_separator():
    print("=" * 60)


def choose_algorithm():
    print_separator()
    print("Wybór algorytmu")
    print_separator()

    for key, value in ALGORITHMS.items():
        print(f"{key}. {value['name']}")

    while True:
        choice = input("\nWybierz algorytm: ")

        if choice in ALGORITHMS:
            return ALGORITHMS[choice]

        print("Niepoprawny wybór")

def choose_table():
    print_separator()
    print("Wybór zbioru danych")
    print_separator()

    for key, value in DATA_TABLES.items():
        print(f"{key}. {value[1]}")

    while True:
        choice = input("\nWybierz tabelę: ")

        if choice in DATA_TABLES:
            return DATA_TABLES[choice][0]

        print("Niepoprawny wybór")


def choose_size():
    print_separator()
    print("Wybór rozmiaru danych")
    print_separator()

    for key, value in DATA_SIZES.items():
        print(f"{key}. {value:,}".replace(",", " "))

    while True:
        choice = input("\nWybierz rozmiar danych: ")

        if choice in DATA_SIZES:
            return DATA_SIZES[choice]

        print("Niepoprawny wybór")

def get_available_cores():
    max_cores = mp.cpu_count()

    available = []
    current = 1

    while current <= max_cores:
        available.append(current)
        current *= 2

    return available


def choose_cores():
    print_separator()
    print("Wybór liczby rdzeni")
    print_separator()

    available_cores = get_available_cores()

    for i, core in enumerate(available_cores, start=1):
        print(f"{i}. {core}")

    while True:
        choice = input("\nWybierz liczbę rdzeni: ")

        try:
            choice = int(choice)

            if 1 <= choice <= len(available_cores):
                return available_cores[choice - 1]

        except ValueError:
            pass

        print("Niepoprawny wybór!")


# ==========================================
# Główna logika programu
# ==========================================
def main():
    # Wybór konfiguracji
    algorithm = choose_algorithm()
    table_name = choose_table()
    set_size = choose_size()
    cores = choose_cores()

    # Pobranie danych
    print_separator()
    print("Wczytane dane")
    print_separator()

    print(f"Tabela: {table_name}")
    print(f"Rozmiar danych: {set_size}")
    print(f"Liczba rdzeni: {cores}")

    data = get_data_from_db(table_name, set_size)
    print(f"Pobrano {len(data)} rekordów")

    # Test sekwencyjny
    print_separator()
    print("Test sekwencyjny")
    print_separator()

    sequential_result = profile_function(
        algorithm["sequential"],
        data,
        label=f"{algorithm['name']} - Sequential"
    )

    # Test równoległy
    print_separator()
    print("Test równoległy")
    print_separator()

    if algorithm["name"] == "Parallel QuickSort":
        import math
        max_depth = int(math.log2(cores))

        parallel_result = profile_function(
            algorithm["parallel"],
            data,
            max_depth,
            label=f"{algorithm['name']} - Parallel"
        )

    elif algorithm["name"] == "Parallel MergeSort":
        import math
        max_depth = int(math.log2(cores))

        parallel_result = profile_function(
            algorithm["parallel"],
            data,
            max_depth,
            label=f"{algorithm['name']} - Parallel"
        )

    elif algorithm["name"] == "Parallel BucketSort":

        parallel_result = profile_function(
            algorithm["parallel"],
            data,
            cores,
            label=f"{algorithm['name']} - Parallel"
        )

    else:
        print("Nieobsługiwany algorytm")
        return


    # Weryfikacja poprawności
    print_separator()
    print("Weryfikacja")
    print_separator()

    if sequential_result == parallel_result:
        print("Sortowanie poprawne")
    else:
        print("Błąd: Wyniki sortowania różnią się")



if __name__ == "__main__":
    mp.freeze_support()
    main()