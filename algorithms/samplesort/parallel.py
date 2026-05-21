import multiprocessing as mp

from .utils import split_data, select_samples, choose_pivots, distribute_to_buckets
from algorithms.mergesort.sequential import merge_sort


def local_sort_worker(chunk, sample_count, queue):
    sorted_chunk = merge_sort(chunk)
    samples = select_samples(sorted_chunk, sample_count)
    queue.put((sorted_chunk, samples))


def bucket_sort_worker(bucket, queue):
    queue.put(merge_sort(bucket))


def parallel_sample_sort(arr, cores):
    if len(arr) <= 1:
        return arr

    # # Divide data, local sort and sampling
    chunks = split_data(arr, cores)
    processes = []
    queues = []

    for chunk in chunks:
        q = mp.Queue()

        p = mp.Process(
            target=local_sort_worker,
            args=(chunk, cores, q)
        )

        processes.append(p)
        queues.append(q)
        p.start()

    sorted_chunks = []
    all_samples = []

    for p, q in zip(processes, queues):
        sorted_chunk, samples = q.get()
        sorted_chunks.append(sorted_chunk)
        all_samples.extend(samples)
        p.join()

    # # Choose pivots and redistribution to bucket
    pivots = choose_pivots(all_samples, cores)
    buckets = [[] for _ in range(cores)]

    for chunk in sorted_chunks:
        local_buckets = distribute_to_buckets(chunk, pivots)

        for i in range(cores):
            buckets[i].extend(local_buckets[i])

    # Parallel sort of buckets
    processes = []
    queues = []

    for bucket in buckets:
        q = mp.Queue()

        p = mp.Process(
            target=bucket_sort_worker,
            args=(bucket, q)
        )

        processes.append(p)
        queues.append(q)
        p.start()

    final_sorted = []

    for p, q in zip(processes, queues):
        final_sorted.extend(q.get())
        p.join()

    return final_sorted