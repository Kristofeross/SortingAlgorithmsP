from .utils import split_data, select_samples, choose_pivots, distribute_to_buckets


def sample_sort(arr, parts=4):
    if len(arr) <= 1:
        return arr

    # Divide data, local sort and sampling
    chunks = split_data(arr, parts)
    sorted_chunks = [sorted(chunk) for chunk in chunks]
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
        final_sorted.extend(sorted(bucket))

    return final_sorted