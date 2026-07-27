import math

from .utils import distribute_to_buckets, sort_bucket

def bucket_sort(arr, num_buckets=None):
    if len(arr) == 0:
        return arr

    # Heuristics
    if num_buckets is None:
        num_buckets = int(math.sqrt(len(arr)))

    buckets = distribute_to_buckets(arr, num_buckets)
    result = []

    for bucket in buckets:
        sorted_bucket = sort_bucket(bucket)
        result.extend(sorted_bucket)

    return result