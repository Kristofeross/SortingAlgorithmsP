import math
import multiprocessing as mp
import time
import sqlite3
import random

def get_data_from_db(table_name, set_size, db_path="../dane.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(f"SELECT value FROM {table_name} WHERE set_size = ?", (set_size,))
    rows = cursor.fetchall()
    conn.close()

    return [row[0] for row in rows]

def partition(arr):
    pivot = arr[len(arr) // 2]
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]

    return left, middle, right

def quicksort(arr):
    if len(arr) <= 1:
        return arr

    left, middle, right = partition(arr)

    return quicksort(left) + middle + quicksort(right)

def parallel_quicksort_worker(arr, depth, max_depth):
    if len(arr) <= 1:
        return arr

    if depth >= max_depth:
        return quicksort(arr)

    left, middle, right = partition(arr)

    left_sorted = parallel_quicksort_worker(left, depth+1, max_depth)
    right_sorted = parallel_quicksort_worker(right, depth+1, max_depth)

    return left_sorted + middle + right_sorted

def parallel_quicksort(arr, pool=None, depth=0, max_depth=3):
    if len(arr) <= 1:
        return arr

    if depth >= max_depth or pool is None:
        return quicksort(arr)

    left, middle, right = partition(arr)

    left_result = pool.apply_async(parallel_quicksort_worker, (left, 1, max_depth))
    right_result = pool.apply_async(parallel_quicksort_worker, (right, 1, max_depth))

    return left_result.get() + middle + right_result.get()

if __name__ == "__main__":
    # data = [random.randint(0, 3000) for _ in range(1000)]

    table_name = "random_int"
    set_size = 100000
    data = get_data_from_db(table_name, set_size)

    print("Tabela przed sortowaniem:")
    # print(data)

    available_cores = mp.cpu_count()
    print(f"Dostępne rdzenie: {available_cores}\nWprowadź liczbę rdzeni:")

    try:
        cores = int(input())
    except:
        cores = 1

    if cores > available_cores:
        print(f"Podana liczba rdzeni {cores} jest większa niż dostępne {available_cores}. Ustawiam na {available_cores}.")
        cores = available_cores
    elif cores < 1:
        print("Minimalna liczba rdzeni to 1. Ustawiam na 1.")
        cores = 1

    print(f"Używam {cores} rdzeni")

    # max_depth = cores.bit_length()  # lub np. int(math.log2(cores))
    max_depth = int(math.log2(cores))

    # Sequential quicksort
    start = time.perf_counter()
    sorted_seq = quicksort(data)
    end = time.perf_counter()
    print(f"\nPo sortowaniu sekwencyjnym (czas): {end - start:.6f} s")
    # print(sorted_seq)

    # Parallel quicksort
    with mp.Pool(cores) as pool:
        start = time.perf_counter()
        sorted_par = parallel_quicksort(data, pool, max_depth)
        end = time.perf_counter()
    print(f"\nPo sortowaniu równoległym (czas): {end - start:.6f} s")
    # print(sorted_par)

    # Check correctness
    assert sorted_seq == sorted_par, "Błąd: wyniki sortowania się różnią!"
    print("\nSortowanie poprawne!")