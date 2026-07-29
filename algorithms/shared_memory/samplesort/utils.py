import ctypes
import math
import numpy as np

from multiprocessing import shared_memory

from algorithms.shared_memory.mergesort.sequential import merge_sort


def get_ctype(dtype):
    if dtype is int:
        return ctypes.c_longlong
    elif dtype is float:
        return ctypes.c_double

    raise ValueError("dtype musi być int lub float")


def create_shared_array(data, dtype):
    c_type = get_ctype(dtype)

    shm = shared_memory.SharedMemory(
        create=True,
        size=len(data) * ctypes.sizeof(c_type)
    )

    shared_array = (c_type * len(data)).from_buffer(shm.buf)
    shared_array[:] = data

    return shm, shared_array


def attach_shared_array(name, length, dtype):
    c_type = get_ctype(dtype)
    shm = shared_memory.SharedMemory(name=name)
    shared_array = (c_type * length).from_buffer(shm.buf)

    return shm, shared_array


def close_shared_memory(shm):
    shm.close()


def destroy_shared_memory(shm):
    shm.close()
    shm.unlink()


def split_ranges(length, process_count):
    chunk_size = length // process_count
    ranges = []

    for i in range(process_count):
        start = i * chunk_size

        if i == process_count - 1:
            end = length
        else:
            end = (i + 1) * chunk_size

        ranges.append((start, end))

    return ranges


def select_samples(sorted_chunk, sample_count):
    if len(sorted_chunk) == 0:
        return []

    step = max(1, len(sorted_chunk) // sample_count)

    return sorted_chunk[::step][:sample_count]


def choose_pivots(samples, process_count):
    samples.sort()

    if len(samples) == 0:
        return []

    pivots = []

    if len(samples) < process_count:
        step = max(1, len(samples) // process_count)
    else:
        step = len(samples) // process_count

    for i in range(1, process_count):
        idx = i * step
        if idx >= len(samples):
            break
        pivots.append(samples[idx])

    deduped = []
    for p in pivots:
        if not deduped or p != deduped[-1]:
            deduped.append(p)

    return deduped


def distribute_to_buckets(data, pivots):
    if not pivots:
        return [list(data)]

    arr = np.asarray(data)
    pivots_arr = np.asarray(pivots)

    indices = np.searchsorted(pivots_arr, arr, side='right')

    order = np.argsort(indices, kind='stable')
    sorted_indices = indices[order]
    sorted_values = arr[order]
    boundaries = np.searchsorted(sorted_indices, np.arange(len(pivots) + 2))

    buckets = []
    for i in range(len(pivots) + 1):
        buckets.append(sorted_values[boundaries[i]:boundaries[i + 1]].tolist())

    return buckets


def flatten_buckets(buckets):
    flat_data = []
    bucket_ranges = []
    start = 0

    for bucket in buckets:
        flat_data.extend(bucket)
        end = start + len(bucket) - 1
        bucket_ranges.append((start, end))
        start = end + 1

    return flat_data, bucket_ranges


def split_data(arr, parts):
    chunk_size = len(arr) // parts
    chunks = []

    for i in range(parts):
        start = i * chunk_size

        if i == parts - 1:
            end = len(arr)
        else:
            end = (i + 1) * chunk_size

        chunks.append(arr[start:end])

    return chunks


def split_bucket_ranges(bucket_ranges, process_count):
    n = len(bucket_ranges)
    chunk_size = math.ceil(n / process_count)
    groups = []

    for i in range(0, n, chunk_size):
        groups.append( bucket_ranges[i:i + chunk_size] )

    return groups


def calculate_parts(data_size):
    if data_size < 1_000:
        return 4
    elif data_size < 10_000:
        return 8
    elif data_size < 100_000:
        return 16

    return min( 64, max(16, int(data_size ** 0.25) * 2) )


def sort_bucket(bucket):
    merge_sort(bucket)
    return bucket