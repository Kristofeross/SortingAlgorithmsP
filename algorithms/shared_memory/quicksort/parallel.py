import multiprocessing as mp
import os
import uuid

from algorithms.shared_memory.quicksort.utils import partition, create_shared_array, attach_shared_array, destroy_shared_memory, calculate_min_size
from algorithms.shared_memory.quicksort.sequential import quicksort
from core.process_diagnostics import log_event, get_sequential_reason


def sort_in_place_on_shared(arr, low, high):
    size = high - low + 1
    local = arr[low:high + 1]
    quicksort(local, 0, size - 1)
    arr[low:high + 1] = local


def parallel_quicksort_recursive(arr, shm_name, length, dtype, low, high, depth, max_depth, min_size, cores, event_queue=None,):
    size = high - low + 1

    reason = get_sequential_reason(size, min_size, depth, max_depth,)

    if size <= 1:
        return

    if reason is not None:
        log_event(
            event_queue,
            event="SEQUENTIAL",
            algorithm="Quick Sort",
            cores=cores,
            depth=depth,
            fragment_size=size,
            max_depth=max_depth,
            min_size=min_size,
            low=low,
            high=high,
            reason=reason,
            parent_pid=os.getppid(),
        )

        sort_in_place_on_shared(arr, low, high,)

        return

    local = arr[low:high + 1]
    local_index = partition(local, 0, size - 1,)

    arr[low:high + 1] = local
    index = low + local_index

    left = (low, index - 1,)
    right = (index, high,)

    processes = []

    for part in [left, right]:
        p_low, p_high = part

        if p_low >= p_high:
            continue

        part_size = p_high - p_low + 1

        if part_size > min_size:
            spawn_id = uuid.uuid4().hex if event_queue is not None else None

            p = mp.Process(
                target=parallel_quicksort_worker,
                args=(
                    shm_name,
                    length,
                    dtype,
                    p_low,
                    p_high,
                    depth + 1,
                    max_depth,
                    min_size,
                    cores,
                    event_queue,
                    spawn_id,
                ),
            )

            log_event(
                event_queue,
                event="PROCESS_START_BEGIN",
                algorithm="Quick Sort",
                cores=cores,
                spawn_id=spawn_id,
                depth=depth + 1,
                fragment_size=part_size,
                max_depth=max_depth,
                min_size=min_size,
                low=p_low,
                high=p_high,
                parent_pid=os.getpid(),
            )

            p.start()

            log_event(
                event_queue,
                event="PROCESS_CREATED",
                algorithm="Quick Sort",
                cores=cores,
                spawn_id=spawn_id,
                depth=depth + 1,
                fragment_size=part_size,
                max_depth=max_depth,
                min_size=min_size,
                low=p_low,
                high=p_high,
                parent_pid=os.getpid(),
                child_pid=p.pid,
            )

            processes.append(p)

        else:
            log_event(
                event_queue,
                event="SEQUENTIAL",
                algorithm="Quick Sort",
                cores=cores,
                depth=depth + 1,
                fragment_size=part_size,
                max_depth=max_depth,
                min_size=min_size,
                low=p_low,
                high=p_high,
                reason="cutoff",
                parent_pid=os.getppid(),
            )

            sort_in_place_on_shared(arr, p_low, p_high,)

    for p in processes:
        p.join()


def parallel_quicksort_worker(shm_name, length, dtype, low, high, depth, max_depth, min_size, cores, event_queue=None, spawn_id=None,):
    log_event(
        event_queue,
        event="WORKER_START",
        algorithm="Quick Sort",
        cores=cores,
        spawn_id=spawn_id,
        depth=depth,
        fragment_size=high - low + 1,
        max_depth=max_depth,
        min_size=min_size,
        low=low,
        high=high,
        parent_pid=os.getppid(),
    )

    shm, arr = attach_shared_array(shm_name, length, dtype,)

    try:
        parallel_quicksort_recursive(arr, shm_name, length, dtype, low, high, depth, max_depth, min_size, cores, event_queue,)

    finally:
        log_event(
            event_queue,
            event="WORKER_END",
            algorithm="Quick Sort",
            cores=cores,
            spawn_id=spawn_id,
            depth=depth,
            fragment_size=high - low + 1,
            max_depth=max_depth,
            min_size=min_size,
            low=low,
            high=high,
            parent_pid=os.getppid(),
        )

        del arr
        shm.close()


def parallel_quicksort(data, max_depth, event_queue=None,):
    if len(data) <= 1:
        return data

    dtype = type(data[0])
    cores = 1 << max_depth

    min_size = calculate_min_size(len(data), max_depth,)

    log_event(
        event_queue,
        event="ROOT_START",
        algorithm="Quick Sort",
        cores=cores,
        depth=0,
        fragment_size=len(data),
        max_depth=max_depth,
        min_size=min_size,
        low=0,
        high=len(data) - 1,
        parent_pid=os.getppid(),
    )

    shm, arr = create_shared_array(data, dtype,)

    try:
        parallel_quicksort_recursive( arr, shm.name, len(arr), dtype, 0, len(arr) - 1, 0, max_depth, min_size, cores, event_queue,)

        result = list(arr)

        log_event(
            event_queue,
            event="ROOT_END",
            algorithm="Quick Sort",
            cores=cores,
            depth=0,
            fragment_size=len(data),
            max_depth=max_depth,
            min_size=min_size,
            low=0,
            high=len(data) - 1,
            parent_pid=os.getppid(),
        )

        return result

    finally:
        del arr
        destroy_shared_memory(shm)