# functions found in different versions of parallel quicksort

import math
import multiprocessing as mp
import time
import sqlite3

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

def test_get_data():
    table_name = "random_int"
    set_size = 100000
    data = get_data_from_db(table_name, set_size)
    return data

def test_cores():
    available_cores = mp.cpu_count()
    print(f"Dostępne rdzenie: {available_cores} | Wprowadź liczbę rdzeni:")
    try:
        cores = int(input())
        # cores = 8
    except:
        cores = 1
    if cores > available_cores:
        print(
            f"Podana liczba rdzeni {cores} jest większa niż dostępne {available_cores}. Ustawiam na {available_cores}.")
        cores = available_cores
    elif cores < 1:
        print("Minimalna liczba rdzeni to 1. Ustawiam na 1.")
        cores = 1

    # max_depth = cores.bit_length()  # lub np. int(math.log2(cores))
    # max_depth = int(math.log2(cores))

    return cores

def sequence_quicksot(data):
    start = time.perf_counter()
    sorted_seq = quicksort(data)
    end = time.perf_counter()
    print(f"\nPo sortowaniu sekwencyjnym (czas): {end - start:.6f} s")

    return sorted_seq


