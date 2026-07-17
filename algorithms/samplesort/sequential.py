from .utils import split_data, select_samples, choose_pivots, distribute_to_buckets, get_optimal_parts
from algorithms.mergesort.sequential import merge_sort


def sample_sort(arr, parts=None):
    if len(arr) <= 1:
        return arr

    if parts is None:
        parts = get_optimal_parts(len(arr))

    # Divide data, local sort and sampling
    chunks = split_data(arr, parts)
    sorted_chunks = [merge_sort(chunk) for chunk in chunks]
    all_samples = []

    for chunk in sorted_chunks:
        all_samples.extend(
            select_samples(chunk, parts)
        )

    # Choose pivots and redistribution to bucket
    pivots = choose_pivots(all_samples, parts)
    buckets = [[] for _ in range(parts)]

    for chunk in sorted_chunks:
        local_buckets = distribute_to_buckets(chunk, pivots)

        for i in range(parts):
            buckets[i].extend(local_buckets[i])

    # Final sort
    final_sorted = []

    for bucket in buckets:
        final_sorted.extend(merge_sort(bucket))

    return final_sorted