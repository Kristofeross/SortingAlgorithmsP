import multiprocessing as mp

from core.database import get_data_from_db
from core.benchmark import profile_function
from core.menu import print_separator, choose_algorithm, choose_table, choose_size, choose_cores


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

    elif algorithm["name"] == "Sample Sort":

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