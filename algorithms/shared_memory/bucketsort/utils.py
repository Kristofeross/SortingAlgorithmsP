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


# version without NumPy
# def distribute_to_buckets(arr, bucket_count):
#     if len(arr) == 0:
#         return []
#
#     min_value = min(arr)
#     max_value = max(arr)
#
#     if min_value == max_value:
#         buckets = [[] for _ in range(bucket_count)]
#         buckets[0] = list(arr)
#
#         return buckets
#
#     bucket_range = (max_value - min_value) / bucket_count
#
#     buckets = [[] for _ in range(bucket_count)]
#
#     for value in arr:
#         index = min(
#             bucket_count - 1,
#             int((value - min_value) / bucket_range)
#         )
#
#         buckets[index].append(value)
#
#     return buckets

# version with NumPy - more optimized
def distribute_to_buckets(arr, bucket_count):
    if len(arr) == 0:
        return []

    arr_np = np.asarray(arr)
    min_value = arr_np.min()
    max_value = arr_np.max()

    if min_value == max_value:
        buckets = [[] for _ in range(bucket_count)]
        buckets[0] = list(arr)
        return buckets

    bucket_range = (max_value - min_value) / bucket_count
    indices = np.minimum(
        bucket_count - 1,
        ((arr_np - min_value) / bucket_range).astype(np.int64)
    )

    order = np.argsort(indices, kind='stable')
    sorted_indices = indices[order]
    sorted_values = arr_np[order]

    buckets = [[] for _ in range(bucket_count)]
    boundaries = np.searchsorted(sorted_indices, np.arange(bucket_count + 1))
    for i in range(bucket_count):
        buckets[i] = sorted_values[boundaries[i]:boundaries[i+1]].tolist()

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


def calculate_bucket_count(data_size, process_count):
    return max(
        int(math.sqrt(data_size)),
        process_count * 8
    )


def split_bucket_ranges(bucket_ranges, process_count):
    n = len(bucket_ranges)
    chunk_size = math.ceil(n / process_count)

    groups = []
    for i in range(0, n, chunk_size):
        groups.append(bucket_ranges[i:i + chunk_size])

    return groups


def sort_bucket(bucket):
    merge_sort(bucket)
    return  bucket