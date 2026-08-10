import ctypes
from multiprocessing import shared_memory


# {cores: próg_min_size}
PARALLEL_CUTOFF = {
    2: 50_000,
    4: 100_000,
    8: 300_000,
    16: 600_000,
    32: 1_200_000,
    64: 2_400_000,
    128: 4_800_000,
    # version closer to a multiple of 100 000
    # 2: 50_000,
    # 4: 100_000,
    # 8: 300_000,
    # 16: 600_000,
    # 32: 1_200_000,
    # 64: 2_500_000,
    # 128: 5_000_000,
}


def get_parallel_cutoff(cores):
    return PARALLEL_CUTOFF.get(cores)


def merge(arr, left, mid, right):
    temp = [None] * (right - left + 1)

    i = left
    j = mid + 1
    k = 0

    while i <= mid and j <= right:
        if arr[i] <= arr[j]:
            temp[k] = arr[i]
            i += 1
        else:
            temp[k] = arr[j]
            j += 1

        k += 1

    while i <= mid:
        temp[k] = arr[i]
        i += 1
        k += 1

    while j <= right:
        temp[k] = arr[j]
        j += 1
        k += 1

    arr[left:right + 1] = temp


def get_ctype(dtype):
    if dtype is int:
        return ctypes.c_longlong
    if dtype is float:
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


def calculate_min_size(data_size, max_depth):
    cores = 1 << max_depth

    cutoff = get_parallel_cutoff(cores)
    fallback = max(5000, data_size // (cores * 8))

    if cutoff is None:
        return fallback

    return cutoff

def close_shared_memory(shm):
    shm.close()


def destroy_shared_memory(shm):
    shm.close()
    shm.unlink()