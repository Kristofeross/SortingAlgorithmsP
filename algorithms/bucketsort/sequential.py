import math

# from .utils import distribute_to_buckets
# from algorithms.mergesort.sequential import merge_sort
from .utils import insertion_sort

def bucket_sort(arr, num_buckets=None):
    if len(arr) == 0:
        return arr

    if num_buckets is None:
        num_buckets = int(math.sqrt(len(arr)))

    min_val, max_val = min(arr), max(arr)
    bucket_range = (max_val - min_val + 1) / num_buckets
    buckets = [[] for _ in range(num_buckets)]

    for num in arr:
        index = min(num_buckets - 1, int((num - min_val) / bucket_range))
        buckets[index].append(num)

    sorted_arr = []
    for bucket in buckets:
        # sorted_bucket = merge_sort(bucket)
        sorted_bucket = insertion_sort(bucket)
        sorted_arr.extend(sorted_bucket)

    return sorted_arr