import multiprocessing as mp
import time
import math
import threading

from temp_support import quicksort, partition, test_get_data, test_cores, sequence_quicksot, profile_function
# W1
def parallel_quicksort_worker(arr, depth, max_depth, output_queue):
    if len(arr) <= 1:
        output_queue.put(arr)
        return

    if depth >= max_depth:
        output_queue.put(quicksort(arr))
        return

    left, middle, right = partition(arr)

    left_queue = mp.Queue()
    right_queue = mp.Queue()

    left_proc = mp.Process(target=parallel_quicksort_worker, args=(left, depth + 1, max_depth, left_queue))
    right_proc = mp.Process(target=parallel_quicksort_worker, args=(right, depth + 1, max_depth, right_queue))

    left_proc.start()
    right_proc.start()

    left_sorted = left_queue.get()
    right_sorted = right_queue.get()

    left_proc.join()
    right_proc.join()

    output_queue.put(left_sorted + middle + right_sorted)

def parallel_quicksort(arr, max_depth):
    if len(arr) <= 1:
        return arr

    output_queue = mp.Queue()
    p = mp.Process(target=parallel_quicksort_worker, args=(arr, 0, max_depth, output_queue))
    p.start()
    result = output_queue.get()
    p.join()

    return result

if __name__ == "__main__":
    data = test_get_data()
    cores = test_cores()
    max_depth = int(math.log2(cores))

    # Sorted quicksort
    sorted_seq = profile_function(quicksort, data, label="Sekwencyjny quicksort")

    # Parallel quicksort with Process
    sorted_par = profile_function(parallel_quicksort, data, max_depth, label="Równoległy quicksort")

    # Check correctness
    assert sorted_seq == sorted_par, "Błąd: wyniki sortowania się różnią!"
    print("Sortowanie poprawne!")
