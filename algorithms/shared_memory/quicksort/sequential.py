from algorithms.shared_memory.quicksort.utils import partition


def quicksort(arr, low=0, high=None):
    if high is None:
        high = len(arr) - 1

    if low >= high:
        return

    index = partition(arr, low, high)

    quicksort(arr, low, index - 1)
    quicksort(arr, index, high)