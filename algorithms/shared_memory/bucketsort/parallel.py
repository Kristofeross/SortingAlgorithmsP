import multiprocessing as mp
import os
import uuid

from algorithms.shared_memory.bucketsort.utils import (
    attach_shared_array, create_shared_array, destroy_shared_memory, calculate_bucket_count, distribute_to_buckets, flatten_buckets,
    split_bucket_ranges, sort_bucket, should_run_parallel, should_spawn_for_group, get_group_size_cutoff, get_parallel_size_cutoff,
)
from .sequential import bucket_sort
from core.process_diagnostics import log_event


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

        local[rel_start:rel_end + 1] = sort_bucket( local[rel_start:rel_end + 1] )

    arr[group_start:group_end + 1] = local


def bucket_worker(shm_name, length, dtype, bucket_ranges, process_count, group_index, bucket_count, group_count, event_queue=None, spawn_id=None):
    if not bucket_ranges:
        return

    group_start = bucket_ranges[0][0]
    group_end = bucket_ranges[-1][1]
    group_size = group_end - group_start + 1

    log_event(
        event_queue,
        event="WORKER_START",
        algorithm="Bucket Sort",
        cores=process_count,
        spawn_id=spawn_id,
        fragment_size=group_size,
        phase="bucket_sort",
        group_size=group_size,
        group_index=group_index,
        bucket_count=bucket_count,
        group_count=group_count,
        parallel_cutoff=get_parallel_size_cutoff(process_count),
        group_cutoff=get_group_size_cutoff(process_count),
        parent_pid=os.getppid(),
    )

    shm, arr = attach_shared_array(shm_name, length, dtype,)

    try:
        sort_group(arr, bucket_ranges,)

    finally:
        log_event(
            event_queue,
            event="WORKER_END",
            algorithm="Bucket Sort",
            cores=process_count,
            spawn_id=spawn_id,
            fragment_size=group_size,
            phase="bucket_sort",
            group_size=group_size,
            group_index=group_index,
            bucket_count=bucket_count,
            group_count=group_count,
            parallel_cutoff=get_parallel_size_cutoff(process_count),
            group_cutoff=get_group_size_cutoff(process_count),
            parent_pid=os.getppid(),
        )

        del arr
        shm.close()


def bucket_worker_inline(arr, bucket_ranges,):
    sort_group(arr, bucket_ranges,)


def parallel_bucket_sort(data, process_count, event_queue=None,):
    if len(data) <= 1:
        return data

    data_size = len(data)

    parallel_cutoff = get_parallel_size_cutoff(process_count)
    group_cutoff = get_group_size_cutoff(process_count)

    log_event(
        event_queue,
        event="ROOT_START",
        algorithm="Bucket Sort",
        cores=process_count,
        fragment_size=data_size,
        phase="bucket_sort",
        parallel_cutoff=parallel_cutoff,
        group_cutoff=group_cutoff,
        parent_pid=os.getppid(),
    )

    if not should_run_parallel(data_size, process_count,) or (data_size // process_count) < group_cutoff:
        log_event(
            event_queue,
            event="SEQUENTIAL",
            algorithm="Bucket Sort",
            cores=process_count,
            fragment_size=data_size,
            phase="bucket_sort",
            reason="parallel_cutoff",
            parallel_cutoff=parallel_cutoff,
            group_cutoff=group_cutoff,
            parent_pid=os.getppid(),
        )

        result = bucket_sort(data)

        log_event(
            event_queue,
            event="ROOT_END",
            algorithm="Bucket Sort",
            cores=process_count,
            fragment_size=data_size,
            phase="bucket_sort",
            parallel_cutoff=parallel_cutoff,
            group_cutoff=group_cutoff,
            parent_pid=os.getppid(),
        )

        return result

    dtype = type(data[0])
    bucket_count = calculate_bucket_count(data_size, process_count,)
    buckets = distribute_to_buckets(data, bucket_count,)
    flat_data, bucket_ranges = flatten_buckets(buckets)
    shm, arr = create_shared_array(flat_data, dtype,)

    try:
        bucket_groups = split_bucket_ranges(bucket_ranges, process_count,)
        group_count = len(bucket_groups)

        processes = []
        sequential_groups = []

        for group_index, group in enumerate(bucket_groups):
            if not group:
                continue

            group_start = group[0][0]
            group_end = group[-1][1]

            group_size = group_end - group_start + 1

            if should_spawn_for_group(group_size, process_count,):
                spawn_id = uuid.uuid4().hex if event_queue is not None else None

                process = mp.Process(
                    target=bucket_worker,
                    args=(
                        shm.name,
                        len(arr),
                        dtype,
                        group,
                        process_count,
                        group_index,
                        bucket_count,
                        group_count,
                        event_queue,
                        spawn_id,
                    ),
                )

                log_event(
                    event_queue,
                    event="PROCESS_START_BEGIN",
                    algorithm="Bucket Sort",
                    cores=process_count,
                    spawn_id=spawn_id,
                    fragment_size=group_size,
                    phase="bucket_sort",
                    group_size=group_size,
                    group_index=group_index,
                    bucket_count=bucket_count,
                    group_count=group_count,
                    parallel_cutoff=parallel_cutoff,
                    group_cutoff=group_cutoff,
                    parent_pid=os.getpid(),
                )

                process.start()

                log_event(
                    event_queue,
                    event="PROCESS_CREATED",
                    algorithm="Bucket Sort",
                    cores=process_count,
                    spawn_id=spawn_id,
                    fragment_size=group_size,
                    phase="bucket_sort",
                    group_size=group_size,
                    group_index=group_index,
                    bucket_count=bucket_count,
                    group_count=group_count,
                    parallel_cutoff=parallel_cutoff,
                    group_cutoff=group_cutoff,
                    parent_pid=os.getpid(),
                    child_pid=process.pid,
                )

                processes.append(process )

            else:
                log_event(
                    event_queue,
                    event="SEQUENTIAL",
                    algorithm="Bucket Sort",
                    cores=process_count,
                    fragment_size=group_size,
                    phase="bucket_sort",
                    group_size=group_size,
                    group_index=group_index,
                    bucket_count=bucket_count,
                    group_count=group_count,
                    reason="group_cutoff",
                    parallel_cutoff=parallel_cutoff,
                    group_cutoff=group_cutoff,
                    parent_pid=os.getppid(),
                )

                sequential_groups.append(group)

        for group in sequential_groups:
            bucket_worker_inline(arr, group,)

        for process in processes:
            process.join()

        result = list(arr)

        log_event(
            event_queue,
            event="ROOT_END",
            algorithm="Bucket Sort",
            cores=process_count,
            fragment_size=data_size,
            phase="bucket_sort",
            bucket_count=bucket_count,
            group_count=group_count,
            parallel_cutoff=parallel_cutoff,
            group_cutoff=group_cutoff,
            parent_pid=os.getppid(),
        )

        return result

    finally:
        del arr
        destroy_shared_memory(shm)