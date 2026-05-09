import math
import multiprocessing as mp
import time
import sqlite3

from algorithms.temp_support import (
    quicksort,
    partition,
    sequence_quicksot,
    # profile_function
    )

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

# if __name__ == "__main__":
    # data = test_get_data()
    # cores = test_cores()
    # max_depth = int(math.log2(cores))
    # sorted_seq = sequence_quicksot(data)
    # sorted_seq = profile_function(quicksort, data, label="Sekwencyjny quicksort")

    # Parallel quicksort with Pool
    # with mp.Pool(cores) as pool:
    #     # start = time.perf_counter()
    #     # sorted_par = parallel_quicksort(data, pool, max_depth=max_depth)
    #     # end = time.perf_counter()
    #     sorted_par = profile_function(parallel_quicksort, data, pool, max_depth, label="Równoległy quicksort")
    # # print(f"\nPo sortowaniu równoległym (czas): {end - start:.6f} s")

    # Check correctness
    # assert sorted_seq == sorted_par, "Błąd: wyniki sortowania się różnią!"
    # print("Sortowanie poprawne!")




