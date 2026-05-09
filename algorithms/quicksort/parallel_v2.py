import multiprocessing as mp

from .utils import partition
from .sequential import quicksort

# W2
def parallel_quicksort_worker(q, arr, max_depth, depth):
    result = parallel_quicksort(arr, max_depth, depth)
    q.put(result)

def parallel_quicksort(arr, max_depth, depth=0):
    if len(arr) <= 1:
        return arr
    if depth >= max_depth:
        return quicksort(arr)

    left, middle, right = partition(arr)

    left_queue = mp.Queue()
    right_queue = mp.Queue()

    left_process = mp.Process(target=parallel_quicksort_worker, args=(left_queue, left, max_depth, depth + 1))
    right_process = mp.Process(target=parallel_quicksort_worker, args=(right_queue, right, max_depth, depth + 1))

    left_process.start()
    right_process.start()

    left_sorted = left_queue.get()
    right_sorted = right_queue.get()

    left_process.join()
    right_process.join()

    return left_sorted + middle + right_sorted