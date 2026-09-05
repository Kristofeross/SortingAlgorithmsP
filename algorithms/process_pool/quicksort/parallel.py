from concurrent.futures import ProcessPoolExecutor

from algorithms.process_pool.quicksort.utils import (
    partition, create_shared_array, attach_shared_array,
    destroy_shared_memory, calculate_min_size
)
from algorithms.process_pool.quicksort.sequential import quicksort


def sort_in_place_on_shared(arr, low, high):
    """Bulk-copy: kopiuje fragment do zwykłej listy, sortuje na niej
    (szybki dostęp), zapisuje wynik z powrotem."""
    size = high - low + 1
    local = arr[low:high + 1]
    quicksort(local, 0, size - 1)
    arr[low:high + 1] = local


def generate_segments(arr, low, high, depth, max_depth, min_size, segments):
    """
    Wykonywane WYŁĄCZNIE w procesie głównym, sekwencyjnie, PRZED zleceniem
    czegokolwiek do puli. Rekurencyjnie dzieli zakres [low, high] przez
    partycjonowanie (tanie - tylko przestawianie indeksów, bez sortowania),
    aż fragment osiągnie min_size albo max_depth. Powstałe, w pełni
    niezależne fragmenty trafiają do listy `segments` - to one, jako
    CAŁOŚĆ, zostaną później zlecone workerom z puli.
    """
    size = high - low + 1

    if size <= 1:
        return

    if size <= min_size or depth >= max_depth:
        segments.append((low, high))
        return

    local = arr[low:high + 1]
    local_index = partition(local, 0, size - 1)
    arr[low:high + 1] = local
    index = low + local_index

    for part_low, part_high in [(low, index - 1), (index, high)]:
        if part_low >= part_high:
            continue
        generate_segments(arr, part_low, part_high, depth + 1, max_depth, min_size, segments)


def sort_segment_worker(shm_name, length, dtype, low, high):
    """
    Wykonywane w procesie roboczym z puli. Sortuje przydzielony fragment
    W CAŁOŚCI, lokalnie - zwykła rekurencja Pythona wewnątrz JEDNEGO
    procesu (funkcja `quicksort` z sequential.py). To NIE tworzy żadnych
    nowych zadań w ProcessPoolExecutor i nie czeka na inne future'y.
    """
    shm, arr = attach_shared_array(shm_name, length, dtype)
    try:
        sort_in_place_on_shared(arr, low, high)
    finally:
        del arr
        shm.close()


def parallel_quicksort(data, max_depth):
    if len(data) <= 1:
        return data

    dtype = type(data[0])
    cores = 1 << max_depth
    min_size = calculate_min_size(len(data), max_depth)

    shm, arr = create_shared_array(data, dtype)

    try:
        # ETAP 1: wygenerowanie niezależnych fragmentów (proces główny,
        # sekwencyjnie).
        segments = []
        generate_segments(arr, 0, len(arr) - 1, 0, max_depth, min_size, segments)

        if not segments:
            return list(arr)

        # ETAP 2: wszystkie fragmenty zlecane do puli NARAZ, przez proces
        # główny. Na future.result() czeka PROCES GŁÓWNY - nigdy worker.
        with ProcessPoolExecutor(max_workers=cores) as executor:
            futures = [
                executor.submit(sort_segment_worker, shm.name, len(arr), dtype, low, high)
                for low, high in segments
            ]
            for future in futures:
                future.result()

        return list(arr)

    finally:
        del arr
        destroy_shared_memory(shm)