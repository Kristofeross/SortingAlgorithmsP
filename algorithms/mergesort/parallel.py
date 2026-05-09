import multiprocessing as mp

from .sequential import merge_sort
from .utils import merge

# def merge_sort(arr):
#     if len(arr) <= 1:
#         return arr
#     mid = len(arr) // 2
#     left = merge_sort(arr[:mid])
#     right = merge_sort(arr[mid:])
#
#     return merge(left, right)

# def merge(left, right):
#     result = []
#     i = j = 0
#     while i < len(left) and j < len(right):
#         if left[i] <= right[j]:
#             result.append(left[i])
#             i += 1
#         else:
#             result.append(right[j])
#             j += 1
#     result.extend(left[i:])
#     result.extend(right[j:])
#
#     return result

def parallel_merge_sort_worker(arr, depth, max_depth, output_queue):
    if len(arr) <= 1 or depth >= max_depth:
        output_queue.put(merge_sort(arr))
        return

    mid = len(arr) // 2
    left, right = arr[:mid], arr[mid:]

    left_queue = mp.Queue()
    right_queue = mp.Queue()

    left_proc = mp.Process(target=parallel_merge_sort_worker, args=(left, depth + 1, max_depth, left_queue))
    right_proc = mp.Process(target=parallel_merge_sort_worker, args=(right, depth + 1, max_depth, right_queue))

    left_proc.start()
    right_proc.start()

    left_sorted = left_queue.get()
    right_sorted = right_queue.get()

    left_proc.join()
    right_proc.join()

    output_queue.put(merge(left_sorted, right_sorted))


def parallel_merge_sort(arr, max_depth):
    output_queue = mp.Queue()
    p = mp.Process(target=parallel_merge_sort_worker, args=(arr, 0, max_depth, output_queue))
    p.start()
    result = output_queue.get()
    p.join()

    return result