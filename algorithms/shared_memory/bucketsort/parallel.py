import multiprocessing as mp

from algorithms.shared_memory.bucketsort.utils import (attach_shared_array, create_shared_array, destroy_shared_memory,
                    calculate_bucket_count, distribute_to_buckets, flatten_buckets, split_bucket_ranges, sort_bucket,
                    should_run_parallel, should_spawn_for_group, get_group_size_cutoff)
from .sequential import bucket_sort


def sort_group(arr, bucket_ranges):
    if not bucket_ranges:
        return

    group_start = bucket_ranges[0][0]
    group_end = bucket_ranges[-1][1]

    local = arr[group_start:group_end + 1]

    for start, end in bucket_ranges:
        if start > end:
            continue
        rel_start = start - group_start
        rel_end = end - group_start
        local[rel_start:rel_end + 1] = sort_bucket(local[rel_start:rel_end + 1])

    arr[group_start:group_end + 1] = local


def bucket_worker(shm_name, length, dtype, bucket_ranges):
    shm, arr = attach_shared_array(shm_name, length, dtype)
    try:
        sort_group(arr, bucket_ranges)
    finally:
        del arr
        shm.close()


def bucket_worker_inline(arr, bucket_ranges):
    sort_group(arr, bucket_ranges)


def parallel_bucket_sort(data, process_count):
    if len(data) <= 1:
        return data

    if not should_run_parallel(len(data), process_count) or (len(data) // process_count) < get_group_size_cutoff(process_count):
        return  bucket_sort(data)

    dtype = type(data[0])
    bucket_count = calculate_bucket_count(len(data), process_count)
    buckets = distribute_to_buckets(data, bucket_count)
    flat_data, bucket_ranges = flatten_buckets(buckets)
    shm, arr = create_shared_array(flat_data, dtype)

    try:
        bucket_groups = split_bucket_ranges(bucket_ranges, process_count)
        processes = []
        sequential_groups = []

        for group in bucket_groups:
            if not group:
                continue

            group_size = group[-1][1] - group[0][0] + 1

            if should_spawn_for_group(group_size, process_count):
                process = mp.Process(target=bucket_worker, args=(shm.name, len(arr), dtype, group))
                process.start()
                processes.append(process)
            else:
                sequential_groups.append(group)

        for group in sequential_groups:
            bucket_worker_inline(arr, group)

        for process in processes:
            process.join()

        return list(arr)

    finally:
        del arr
        destroy_shared_memory(shm)