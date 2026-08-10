import multiprocessing as mp

from algorithms.shared_memory.bucketsort.utils import (attach_shared_array, create_shared_array, destroy_shared_memory,
                    calculate_bucket_count, distribute_to_buckets, flatten_buckets, split_bucket_ranges, sort_bucket,
                    should_run_parallel, should_spawn_for_group, get_group_size_cutoff)


# version with min group size
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
# end

# version without min group size - less optimized
# def bucket_worker(shm_name, length, dtype, bucket_ranges):
#     if not bucket_ranges:
#         return
#
#     shm, arr = attach_shared_array(shm_name, length, dtype)
#
#     try:
#         group_start = bucket_ranges[0][0]
#         group_end = bucket_ranges[-1][1]
#
#         local = arr[group_start:group_end + 1]
#
#         for start, end in bucket_ranges:
#             if start > end:
#                 continue
#             rel_start = start - group_start
#             rel_end = end - group_start
#             local[rel_start:rel_end + 1] = sort_bucket(local[rel_start:rel_end + 1])
#
#         arr[group_start:group_end + 1] = local
#
#     finally:
#         del arr
#         shm.close()


def parallel_bucket_sort(data, process_count):
    if len(data) <= 1:
        return data

    # version with min group size - more optimized
    # if len(data) <= MIN_SIZE_FOR_PARALLEL or (len(data) // process_count) < MIN_GROUP_SIZE:
    #     return sort_bucket(data)
    # if not should_run_parallel(len(data)) or (len(data) // process_count) < MIN_GROUP_SIZE:
    #     return sort_bucket(data) # almast done
    # end


    # the newest attempt
    if not should_run_parallel(len(data), process_count) or (len(data) // process_count) < get_group_size_cutoff(process_count):
        return sort_bucket(data)
    # ex

    dtype = type(data[0])
    bucket_count = calculate_bucket_count(len(data), process_count)
    buckets = distribute_to_buckets(data, bucket_count)
    flat_data, bucket_ranges = flatten_buckets(buckets)
    shm, arr = create_shared_array(flat_data, dtype)

    try:
        bucket_groups = split_bucket_ranges(bucket_ranges, process_count)
        processes = []
        sequential_groups = [] # version with min group size - more optimized

        for group in bucket_groups:
            if not group:
                continue

            # old version without min group size - less optimized
            # process = mp.Process(
            #     target=bucket_worker,
            #     args=(
            #         shm.name,
            #         len(arr),
            #         dtype,
            #         group
            #     )
            # )
            #
            # process.start()
            # processes.append(process)
            # end old version without min group size - less optimized

            # version without min group size - more optimized
            group_size = group[-1][1] - group[0][0] + 1

            if should_spawn_for_group(group_size, process_count):
                process = mp.Process(target=bucket_worker, args=(shm.name, len(arr), dtype, group))
                process.start()
                processes.append(process)
            else:
                sequential_groups.append(group)

        for group in sequential_groups:
            bucket_worker_inline(arr, group)
        # end version without min group size - more optimized

        for process in processes:
            process.join()

        return list(arr)

    finally:
        del arr
        destroy_shared_memory(shm)