import math

from .utils import insertion_sort, distribute_to_buckets

def bucket_sort(arr, num_buckets=None):
    if len(arr) == 0:
        return arr

    # Heuristics
    if num_buckets is None:
        num_buckets = int(math.sqrt(len(arr)))

    buckets = distribute_to_buckets(arr, num_buckets)
    sorted_arr = []

    for bucket in buckets:
        sorted_bucket = insertion_sort(bucket)
        sorted_arr.extend(sorted_bucket)

    return sorted_arr