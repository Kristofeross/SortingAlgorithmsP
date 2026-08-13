import multiprocessing as mp

from algorithms.shared_memory.quicksort.utils import partition, create_shared_array, attach_shared_array, destroy_shared_memory, calculate_min_size
from algorithms.shared_memory.quicksort.sequential import quicksort


def sort_in_place_on_shared(arr, low, high):
    size = high - low + 1
    local = arr[low:high + 1]
    quicksort(local, 0, size - 1)
    arr[low:high + 1] = local


def parallel_quicksort_recursive(arr, shm_name, length, dtype, low, high, depth, max_depth, min_size):
    size = high - low + 1

    if size <= 1:
        return

    if size <= min_size or depth >= max_depth:
        sort_in_place_on_shared(arr, low, high)
        return

    local = arr[low:high + 1]
    local_index = partition(local, 0, size - 1)
    arr[low:high + 1] = local
    index = low + local_index

    left = (low, index - 1)
    right = (index, high)

    processes = []

    for part in [left, right]:
        p_low, p_high = part

        if p_low >= p_high:
            continue


        part_size = p_high - p_low + 1

        if part_size > min_size:
            p = mp.Process(
                target=parallel_quicksort_worker,
                args=(shm_name, length, dtype, p_low, p_high, depth + 1, max_depth, min_size)
            )
            p.start()
            processes.append(p)
        else:
            sort_in_place_on_shared(arr, p_low, p_high)

    for p in processes:
        p.join()


def parallel_quicksort_worker(shm_name, length, dtype, low, high, depth, max_depth, min_size):
    shm, arr = attach_shared_array(shm_name, length, dtype)

    try:
        parallel_quicksort_recursive(
            arr,
            shm_name,
            length,
            dtype,
            low,
            high,
            depth,
            max_depth,
            min_size
        )

    finally:
        del arr
        shm.close()


def parallel_quicksort(data, max_depth):
    if len(data) <= 1:
        return data

    dtype = type(data[0])

    min_size = calculate_min_size(len(data), max_depth)
    shm, arr = create_shared_array(data, dtype)

    try:
        parallel_quicksort_recursive(
            arr,
            shm.name,
            len(arr),
            dtype,
            0,
            len(arr)-1,
            0,
            max_depth,
            min_size
        )

        return list(arr)

    finally:
        del arr
        destroy_shared_memory(shm)