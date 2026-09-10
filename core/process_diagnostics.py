import os
import time


def create_event(
    event,
    algorithm,
    cores,

    # Proces
    spawn_id=None,

    fragment_size=None,
    reason=None,
    parent_pid=None,
    child_pid=None,

    # Quick Sort / Merge Sort
    depth=None,
    max_depth=None,
    min_size=None,
    low=None,
    high=None,

    # Bucket Sort / Sample Sort
    phase=None,
    group_size=None,
    group_index=None,
    bucket_count=None,
    group_count=None,
    parallel_cutoff=None,
    group_cutoff=None,
):
    return {
        "event": event,
        "algorithm": algorithm,
        "cores": cores,

        "spawn_id": spawn_id,

        "pid": os.getpid(),
        "parent_pid": parent_pid,
        "child_pid": child_pid,

        "fragment_size": fragment_size,
        "reason": reason,

        "depth": depth,
        "max_depth": max_depth,
        "min_size": min_size,
        "low": low,
        "high": high,

        "phase": phase,
        "group_size": group_size,
        "group_index": group_index,
        "bucket_count": bucket_count,
        "group_count": group_count,
        "parallel_cutoff": parallel_cutoff,
        "group_cutoff": group_cutoff,

        "timestamp": time.perf_counter(),
    }


def log_event(event_queue, **event_data):
    if event_queue is None:
        return

    event_queue.put(create_event(**event_data))


def get_sequential_reason(size, min_size, depth, max_depth,):
    cutoff_reached = size <= min_size
    depth_reached = depth >= max_depth

    if cutoff_reached and depth_reached:
        return "cutoff_and_max_depth"

    if cutoff_reached:
        return "cutoff"

    if depth_reached:
        return "max_depth"

    return None