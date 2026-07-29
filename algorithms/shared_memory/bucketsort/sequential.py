import math

from algorithms.shared_memory.bucketsort.utils import distribute_to_buckets, sort_bucket


def bucket_sort(arr, bucket_count=None):
    if len(arr) <= 1:
        return arr

    if bucket_count is None:
        bucket_count = int(math.sqrt(len(arr)))

    buckets = distribute_to_buckets(arr, bucket_count)
    sorted_array = []

    for bucket in buckets:
        sorted_array.extend(sort_bucket(bucket))

    return sorted_array