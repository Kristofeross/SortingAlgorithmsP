import multiprocessing as mp
import os
import uuid

from .utils import (
    attach_shared_array, create_shared_array, destroy_shared_memory, split_ranges, select_samples, choose_pivots, distribute_to_buckets,
    flatten_buckets, split_bucket_ranges, sort_bucket, should_run_parallel, should_spawn_for_group, get_group_size_cutoff, get_parallel_size_cutoff,
)
from .sequential import sample_sort
from core.process_diagnostics import log_event


def local_sort_worker(shm_name, length, dtype, start, end, process_count, group_index, group_count, event_queue=None, spawn_id=None,):
    fragment_size = end - start

    log_event(
        event_queue,
        event="WORKER_START",
        algorithm="Sample Sort",
        cores=process_count,
        spawn_id=spawn_id,
        fragment_size=fragment_size,
        phase="local_sort",
        group_size=fragment_size,
        group_index=group_index,
        group_count=group_count,
        parallel_cutoff=get_parallel_size_cutoff(process_count),
        group_cutoff=get_group_size_cutoff(process_count),
        parent_pid=os.getppid(),
    )

    shm, arr = attach_shared_array(shm_name, length, dtype,)

    try:
        local = list(arr[start:end])
        local = sort_bucket(local)
        arr[start:end] = local

    finally:
        log_event(
            event_queue,
            event="WORKER_END",
            algorithm="Sample Sort",
            cores=process_count,
            spawn_id=spawn_id,
            fragment_size=fragment_size,
            phase="local_sort",
            group_size=fragment_size,
            group_index=group_index,
            group_count=group_count,
            parallel_cutoff=get_parallel_size_cutoff(process_count),
            group_cutoff=get_group_size_cutoff(process_count),
            parent_pid=os.getppid(),
        )

        del arr
        shm.close()


def sort_group(arr, bucket_ranges,):
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


def bucket_worker(shm_name, length, dtype, bucket_ranges, process_count, group_index, bucket_count, group_count, event_queue=None, spawn_id=None):
    if not bucket_ranges:
        return

    group_start = bucket_ranges[0][0]
    group_end = bucket_ranges[-1][1]

    group_size = group_end - group_start + 1

    log_event(
        event_queue,
        event="WORKER_START",
        algorithm="Sample Sort",
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
            algorithm="Sample Sort",
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


def parallel_sample_sort(data, process_count, event_queue=None,):
    if len(data) <= 1:
        return data

    data_size = len(data)

    parallel_cutoff = get_parallel_size_cutoff(process_count)

    group_cutoff = get_group_size_cutoff(process_count)

    log_event(
        event_queue,
        event="ROOT_START",
        algorithm="Sample Sort",
        cores=process_count,
        fragment_size=data_size,
        parallel_cutoff=parallel_cutoff,
        group_cutoff=group_cutoff,
        parent_pid=os.getppid(),
    )

    if not should_run_parallel(data_size, process_count,):
        log_event(
            event_queue,
            event="SEQUENTIAL",
            algorithm="Sample Sort",
            cores=process_count,
            fragment_size=data_size,
            reason="parallel_cutoff",
            parallel_cutoff=parallel_cutoff,
            group_cutoff=group_cutoff,
            parent_pid=os.getpid(),
        )

        result = sample_sort(data, process_count)

        log_event(
            event_queue,
            event="ROOT_END",
            algorithm="Sample Sort",
            cores=process_count,
            fragment_size=data_size,
            parallel_cutoff=parallel_cutoff,
            group_cutoff=group_cutoff,
            parent_pid=os.getppid(),
        )

        return result

    dtype = type(data[0])
    shm, arr = create_shared_array(data, dtype,)

    try:
        ranges = split_ranges(data_size, process_count,)
        processes = []

        local_group_count = len(ranges)

        for group_index, (start, end) in enumerate(ranges):
            fragment_size = (end - start)

            spawn_id = uuid.uuid4().hex if event_queue is not None else None

            process = mp.Process(
                target=local_sort_worker,
                args=(
                    shm.name,
                    len(arr),
                    dtype,
                    start,
                    end,
                    process_count,
                    group_index,
                    local_group_count,
                    event_queue,
                    spawn_id,
                ),
            )

            log_event(
                event_queue,
                event="PROCESS_START_BEGIN",
                algorithm="Sample Sort",
                cores=process_count,
                spawn_id=spawn_id,
                fragment_size=fragment_size,
                phase="local_sort",
                group_size=fragment_size,
                group_index=group_index,
                group_count=local_group_count,
                parallel_cutoff=parallel_cutoff,
                group_cutoff=group_cutoff,
                parent_pid=os.getpid(),
            )

            process.start()

            log_event(
                event_queue,
                event="PROCESS_CREATED",
                algorithm="Sample Sort",
                cores=process_count,
                spawn_id=spawn_id,
                fragment_size=fragment_size,
                phase="local_sort",
                group_size=fragment_size,
                group_index=group_index,
                group_count=local_group_count,
                parallel_cutoff=parallel_cutoff,
                group_cutoff=group_cutoff,
                parent_pid=os.getpid(),
                child_pid=process.pid,
            )

            processes.append(process)

        for process in processes:
            process.join()

        samples = []

        for start, end in ranges:
            samples.extend( select_samples(arr[start:end], process_count,) )

        pivots = choose_pivots(samples, process_count,)

        buckets = [
            [] for _ in range(len(pivots) + 1)
        ]

        for start, end in ranges:
            local_buckets = distribute_to_buckets(arr[start:end], pivots,)

            for i in range( len(local_buckets) ):
                buckets[i].extend(local_buckets[i])

    finally:
        del arr
        destroy_shared_memory(shm)


    flat_data, bucket_ranges = flatten_buckets(buckets)
    shm, arr = create_shared_array(flat_data, dtype,)

    try:
        bucket_groups = split_bucket_ranges(bucket_ranges, process_count,)

        processes = []
        sequential_groups = []

        bucket_group_count = len(bucket_groups)

        bucket_count = len(bucket_ranges)

        for group_index, group in enumerate(bucket_groups):
            if not group:
                continue

            group_start = group[0][0]
            group_end = group[-1][1]

            group_size = group_end - group_start + 1

            if should_spawn_for_group(group_size, process_count):
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
                        bucket_group_count,
                        event_queue,
                        spawn_id,
                    ),
                )

                log_event(
                    event_queue,
                    event="PROCESS_START_BEGIN",
                    algorithm="Sample Sort",
                    cores=process_count,
                    spawn_id=spawn_id,
                    fragment_size=group_size,
                    phase="bucket_sort",
                    group_size=group_size,
                    group_index=group_index,
                    bucket_count=bucket_count,
                    group_count=bucket_group_count,
                    parallel_cutoff=parallel_cutoff,
                    group_cutoff=group_cutoff,
                    parent_pid=os.getpid(),
                )

                process.start()

                log_event(
                    event_queue,
                    event="PROCESS_CREATED",
                    algorithm="Sample Sort",
                    cores=process_count,
                    spawn_id=spawn_id,
                    fragment_size=group_size,
                    phase="bucket_sort",
                    group_size=group_size,
                    group_index=group_index,
                    bucket_count=bucket_count,
                    group_count=bucket_group_count,
                    parallel_cutoff=parallel_cutoff,
                    group_cutoff=group_cutoff,
                    parent_pid=os.getpid(),
                    child_pid=process.pid,
                )

                processes.append(process)

            else:
                log_event(
                    event_queue,
                    event="SEQUENTIAL",
                    algorithm="Sample Sort",
                    cores=process_count,
                    fragment_size=group_size,
                    phase="bucket_sort",
                    group_size=group_size,
                    group_index=group_index,
                    bucket_count=bucket_count,
                    group_count=bucket_group_count,
                    reason="group_cutoff",
                    parallel_cutoff=parallel_cutoff,
                    group_cutoff=group_cutoff,
                    parent_pid=os.getpid(),
                )

                sequential_groups.append(group)

        for group in sequential_groups:
            sort_group(arr, group,)

        for process in processes:
            process.join()

        result = list(arr)

        log_event(
            event_queue,
            event="ROOT_END",
            algorithm="Sample Sort",
            cores=process_count,
            fragment_size=data_size,
            bucket_count=bucket_count,
            group_count=bucket_group_count,
            parallel_cutoff=parallel_cutoff,
            group_cutoff=group_cutoff,
            parent_pid=os.getppid(),
        )

        return result

    finally:
        del arr
        destroy_shared_memory(shm)