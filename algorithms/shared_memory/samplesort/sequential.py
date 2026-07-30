from .utils import split_data, select_samples, choose_pivots, distribute_to_buckets, sort_bucket, calculate_parts


def sample_sort(arr, parts=None):
    if len(arr) <= 1:
        return arr

    if parts is None:
        parts = calculate_parts(len(arr))

    chunks = split_data(arr, parts)

    sorted_chunks = []
    samples = []

    for chunk in chunks:
        sorted_chunk = sort_bucket(chunk)
        sorted_chunks.append(sorted_chunk)
        samples.extend( select_samples(sorted_chunk, parts) )

    pivots = choose_pivots(samples, parts)
    buckets = [[] for _ in range(len(pivots) + 1)]

    for chunk in sorted_chunks:
        local_buckets = distribute_to_buckets(chunk, pivots)

        for i in range(len(local_buckets)):
            buckets[i].extend(local_buckets[i])

    sorted_array = []

    for bucket in buckets:
        sorted_array.extend(sort_bucket(bucket))

    return sorted_array