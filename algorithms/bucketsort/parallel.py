import multiprocessing as mp

from .utils import insertion_sort, distribute_to_buckets


def parallel_bucket_sort_worker(bucket, output_queue):
    sorted_bucket = insertion_sort(bucket)
    output_queue.put(sorted_bucket)

def parallel_bucket_sort(arr, num_buckets):
    if len(arr) == 0:
        return arr

    buckets = distribute_to_buckets(arr, num_buckets)

    processes = []
    queues = []

    # Creating process
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

    # Merging
    sorted_arr = []

    for bucket in sorted_buckets:
        sorted_arr.extend(bucket)

    return sorted_arr