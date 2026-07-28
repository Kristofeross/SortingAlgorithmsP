import multiprocessing as mp

from algorithms.shared_memory.mergesort.utils import merge, create_shared_array, attach_shared_array, destroy_shared_memory, calculate_min_size
from algorithms.shared_memory.mergesort.sequential import merge_sort


def sort_in_place_on_shared(arr, left, right):
    size = right - left + 1
    local = arr[left:right + 1]
    merge_sort(local, 0, size - 1)
    arr[left:right + 1] = local


def parallel_mergesort_recursive(arr, shm_name, length, dtype, left, right, depth, max_depth, min_size,):
    size = right - left + 1

    if size <= 1:
        return

    if size <= min_size or depth >= max_depth:
        sort_in_place_on_shared(arr, left, right)
        return

    mid = (left + right) // 2
    processes = []


    for part_left, part_right in [(left, mid), (mid + 1, right)]:
        part_size = part_right - part_left + 1

        if part_size > min_size:
            p = mp.Process(
                target=parallel_mergesort_worker,
                args=(
                    shm_name,
                    length,
                    dtype,
                    part_left,
                    part_right,
                    depth + 1,
                    max_depth,
                    min_size
                )
            )
            p.start()
            processes.append(p)
        else:
            sort_in_place_on_shared(arr, part_left, part_right)


    for process in processes:
        process.join()

    local = arr[left:right + 1]
    merge(local, 0, mid - left, size - 1)
    arr[left:right + 1] = local


def parallel_mergesort_worker(shm_name, length, dtype, left, right, depth, max_depth, min_size):
    shm, arr = attach_shared_array(
        shm_name,
        length,
        dtype,
    )

    try:
        parallel_mergesort_recursive(
            arr,
            shm_name,
            length,
            dtype,
            left,
            right,
            depth,
            max_depth,
            min_size,
        )
    finally:
        del arr
        shm.close()


def parallel_merge_sort(data, max_depth):
    if len(data) <= 1:
        return data

    dtype = type(data[0])

    min_size = calculate_min_size(
        len(data),
        max_depth,
    )

    shm, arr = create_shared_array(
        data,
        dtype,
    )

    try:
        parallel_mergesort_recursive(
            arr,
            shm.name,
            len(arr),
            dtype,
            0,
            len(arr) - 1,
            0,
            max_depth,
            min_size,
        )

        return list(arr)

    finally:
        del arr
        destroy_shared_memory(shm)