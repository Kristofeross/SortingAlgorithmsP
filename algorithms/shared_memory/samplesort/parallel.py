import multiprocessing as mp

from .utils import (attach_shared_array, create_shared_array, destroy_shared_memory, split_ranges, select_samples,
                    choose_pivots, distribute_to_buckets, flatten_buckets, split_bucket_ranges, sort_bucket, should_run_parallel, MIN_GROUP_SIZE)
from .sequential import sample_sort


def local_sort_worker(shm_name, length, dtype, start, end):
    shm, arr = attach_shared_array(shm_name, length, dtype)
    try:
        local = list(arr[start:end])
        local = sort_bucket(local)
        arr[start:end] = local
    finally:
        del arr
        shm.close()


def sort_group(arr, bucket_ranges):
    if not bucket_ranges:
        return

    group_start = bucket_ranges[0][0]
    group_end = bucket_ranges[-1][1]
    local = list(arr[group_start:group_end + 1])

    for start, end in bucket_ranges:
        if start > end:
            continue
        rel_start = start - group_start
        rel_end = end - group_start
        local[rel_start:rel_end + 1] = sort_bucket(local[rel_start:rel_end + 1])

    arr[group_start:group_end + 1] = local


def bucket_worker(shm_name, length, dtype, bucket_ranges):
    if not bucket_ranges:
        return

    shm, arr = attach_shared_array(shm_name, length, dtype)
    try:
        sort_group(arr, bucket_ranges)
    finally:
        del arr
        shm.close()


def parallel_sample_sort(data, process_count):
    if len(data) <= 1:
        return data

    if not should_run_parallel(len(data), process_count):
        return sample_sort(data)

    dtype = type(data[0])
    shm, arr = create_shared_array(data, dtype)

    try:
        ranges = split_ranges(len(data), process_count)
        processes = []

        for start, end in ranges:
            process = mp.Process(
                target=local_sort_worker,
                args=(shm.name, len(arr), dtype, start, end)
            )
            process.start()
            processes.append(process)

        for process in processes:
            process.join()

        samples = []

        for start, end in ranges:
            samples.extend(select_samples(arr[start:end], process_count))

        pivots = choose_pivots(samples, process_count)

        buckets = [[] for _ in range(len(pivots) + 1)]

        for start, end in ranges:
            local_buckets = distribute_to_buckets(arr[start:end], pivots)

            for i in range(len(local_buckets)):
                buckets[i].extend(local_buckets[i])

    finally:
        del arr
        destroy_shared_memory(shm)

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

            if group_size > MIN_GROUP_SIZE:
                process = mp.Process(
                    target=bucket_worker,
                    args=(shm.name, len(arr), dtype, group)
                )
                process.start()
                processes.append(process)
            else:
                sequential_groups.append(group)

        for group in sequential_groups:
            sort_group(arr, group)

        for process in processes:
            process.join()

        return list(arr)

    finally:
        del arr
        destroy_shared_memory(shm)