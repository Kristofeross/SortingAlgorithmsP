import multiprocessing as mp

from .utils import distribute_to_buckets, calculate_bucket_count, split_bucket_groups, sort_bucket


def parallel_bucket_sort_worker(bucket_group, output_queue):
    result = []

    for index, bucket in bucket_group:
        sorted_bucket = sort_bucket(bucket)
        result.append((index, sorted_bucket))

    output_queue.put(result)


def parallel_bucket_sort(arr, process_count):
    if len(arr) <= 1:
        return arr

    bucket_count = calculate_bucket_count(len(arr), process_count)
    buckets = distribute_to_buckets(arr, bucket_count)
    bucket_groups = split_bucket_groups(buckets, process_count)

    processes = []
    queues = []

    for group in bucket_groups:
        queue = mp.Queue()

        process = mp.Process(
            target=parallel_bucket_sort_worker,
            args=(group, queue)
        )

        process.start()

        processes.append(process)
        queues.append(queue)

    sorted_buckets = [None] * bucket_count

    for process, queue in zip(processes, queues):
        group_result = queue.get()

        for bucket_index, bucket in group_result:
            sorted_buckets[bucket_index] = bucket

        process.join()

    sorted_arr = []

    for bucket in sorted_buckets:
        sorted_arr.extend(bucket)

    return sorted_arr