import  multiprocessing as mp
from .config import ALGORITHMS, DATA_SIZES, DATA_TABLES


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