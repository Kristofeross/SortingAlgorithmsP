import multiprocessing as mp

from .sequential import insertion_sort
# from algorithms.mergesort.sequential import merge_sort

# def insertion_sort(arr):
#     for i in range(1, len(arr)):
#         key = arr[i]
#         j = i - 1
#         while j >= 0 and arr[j] > key:
#             arr[j + 1] = arr[j]
#             j -= 1
#         arr[j + 1] = key
#
#     return arr

# def bucket_sort(arr, num_buckets=None):
#     if len(arr) == 0:
#         return arr
#
#     if num_buckets is None:
#         num_buckets = int(math.sqrt(len(arr)))
#
#     min_val, max_val = min(arr), max(arr)
#     bucket_range = (max_val - min_val + 1) / num_buckets
#     buckets = [[] for _ in range(num_buckets)]
#
#
#     for num in arr:
#         index = min(num_buckets - 1, int((num - min_val) / bucket_range))
#         buckets[index].append(num)
#
#
#     sorted_arr = []
#     for bucket in buckets:
#         sorted_bucket = merge_sort(bucket)
#         sorted_arr.extend(sorted_bucket)
#
#     return sorted_arr

def parallel_bucket_sort_worker(bucket, output_queue):
    sorted_bucket = insertion_sort(bucket)
    # sorted_bucket = merge_sort(bucket)
    output_queue.put(sorted_bucket)

def parallel_bucket_sort(arr, num_buckets):
    if len(arr) == 0:
        return arr

    min_val, max_val = min(arr), max(arr)
    bucket_range = (max_val - min_val + 1) / num_buckets
    buckets = [[] for _ in range(num_buckets)]

    for num in arr:
        index = min(num_buckets - 1, int((num - min_val) / bucket_range))
        buckets[index].append(num)

    processes = []
    queues = []
    for bucket in buckets:
        q = mp.Queue()
        p = mp.Process(target=parallel_bucket_sort_worker, args=(bucket, q))
        processes.append(p)
        queues.append(q)
        p.start()

    sorted_buckets = []
    for p, q in zip(processes, queues):
        sorted_buckets.append(q.get())
        p.join()

    sorted_arr = []
    for bucket in sorted_buckets:
        sorted_arr.extend(bucket)

    return sorted_arr