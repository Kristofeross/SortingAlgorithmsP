import ctypes

from multiprocessing import shared_memory


def merge(arr, left, mid, right):
    temp = []

    i = left
    j = mid + 1

    while i <= mid and j <= right:
        if arr[i] <= arr[j]:
            temp.append(arr[i])
            i += 1
        else:
            temp.append(arr[j])
            j += 1

    while i <= mid:
        temp.append(arr[i])
        i += 1

    while j <= right:
        temp.append(arr[j])
        j += 1

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

    return max(5000, data_size // (cores * 8))
    # return max(4000, data_size // (cores * 6))
    # return max(3000, data_size // (cores * 4))
    # return max(20000, data_size // (cores * 4))
    # return max(50000, data_size // (cores * 2))


def close_shared_memory(shm):
    shm.close()


def destroy_shared_memory(shm):
    shm.close()
    shm.unlink()