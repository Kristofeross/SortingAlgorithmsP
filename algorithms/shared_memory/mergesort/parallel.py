import multiprocessing as mp
import os
import uuid

from algorithms.shared_memory.mergesort.utils import merge, create_shared_array, attach_shared_array, destroy_shared_memory, calculate_min_size
from algorithms.shared_memory.mergesort.sequential import merge_sort
from core.process_diagnostics import log_event, get_sequential_reason


def sort_in_place_on_shared(arr, left, right):
    size = right - left + 1
    local = arr[left:right + 1]
    merge_sort(local, 0, size - 1)
    arr[left:right + 1] = local


def parallel_mergesort_recursive(arr, shm_name, length, dtype, left, right, depth, max_depth, min_size, cores, event_queue=None,):
    size = right - left + 1

    reason = get_sequential_reason(size, min_size, depth, max_depth,)

    if size <= 1:
        return

    if reason is not None:
        log_event(
            event_queue,
            event="SEQUENTIAL",
            algorithm="Merge Sort",
            cores=cores,
            depth=depth,
            fragment_size=size,
            max_depth=max_depth,
            min_size=min_size,
            low=left,
            high=right,
            reason=reason,
            parent_pid=os.getppid(),
        )

        sort_in_place_on_shared(arr, left, right,)

        return

    mid = (left + right) // 2

    processes = []

    for part_left, part_right in [ (left, mid), (mid + 1, right), ]:
        part_size = part_right - part_left + 1

        if part_size > min_size:
            spawn_id = uuid.uuid4().hex if event_queue is not None else None

            process = mp.Process(
                target=parallel_mergesort_worker,
                args=(
                    shm_name,
                    length,
                    dtype,
                    part_left,
                    part_right,
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
                algorithm="Merge Sort",
                cores=cores,
                spawn_id=spawn_id,
                depth=depth + 1,
                fragment_size=part_size,
                max_depth=max_depth,
                min_size=min_size,
                low=part_left,
                high=part_right,
                parent_pid=os.getpid(),
            )

            process.start()

            log_event(
                event_queue,
                event="PROCESS_CREATED",
                algorithm="Merge Sort",
                cores=cores,
                spawn_id=spawn_id,
                depth=depth + 1,
                fragment_size=part_size,
                max_depth=max_depth,
                min_size=min_size,
                low=part_left,
                high=part_right,
                parent_pid=os.getpid(),
                child_pid=process.pid,
            )

            processes.append(process)

        else:
            log_event(
                event_queue,
                event="SEQUENTIAL",
                algorithm="Merge Sort",
                cores=cores,
                depth=depth + 1,
                fragment_size=part_size,
                max_depth=max_depth,
                min_size=min_size,
                low=part_left,
                high=part_right,
                reason="cutoff",
                parent_pid=os.getppid(),
            )

            sort_in_place_on_shared(arr, part_left, part_right,)

    for process in processes:
        process.join()

    local = arr[left:right + 1]
    merge(local, 0, mid - left, size - 1,)
    arr[left:right + 1] = local


def parallel_mergesort_worker( shm_name, length, dtype, left, right, depth, max_depth, min_size, cores, event_queue=None, spawn_id=None,):
    log_event(
        event_queue,
        event="WORKER_START",
        algorithm="Merge Sort",
        cores=cores,
        spawn_id=spawn_id,
        depth=depth,
        fragment_size=right - left + 1,
        max_depth=max_depth,
        min_size=min_size,
        low=left,
        high=right,
        parent_pid=os.getppid(),
    )

    shm, arr = attach_shared_array(shm_name, length, dtype,)

    try:
        parallel_mergesort_recursive(arr, shm_name, length, dtype, left, right, depth, max_depth, min_size, cores, event_queue,)

    finally:
        log_event(
            event_queue,
            event="WORKER_END",
            algorithm="Merge Sort",
            cores=cores,
            spawn_id=spawn_id,
            depth=depth,
            fragment_size=right - left + 1,
            max_depth=max_depth,
            min_size=min_size,
            low=left,
            high=right,
            parent_pid=os.getppid(),
        )

        del arr
        shm.close()


def parallel_merge_sort( data, max_depth, event_queue=None,):
    if len(data) <= 1:
        return data

    dtype = type(data[0])
    cores = 1 << max_depth
    min_size = calculate_min_size(len(data), max_depth,)

    log_event(
        event_queue,
        event="ROOT_START",
        algorithm="Merge Sort",
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
        parallel_mergesort_recursive(arr, shm.name, len(arr), dtype, 0, len(arr) - 1, 0, max_depth, min_size, cores, event_queue,)

        result = list(arr)

        log_event(
            event_queue,
            event="ROOT_END",
            algorithm="Merge Sort",
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