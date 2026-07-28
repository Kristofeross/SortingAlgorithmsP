from algorithms.shared_memory.mergesort.utils import merge


def merge_sort(arr, left=0, right=None):
    if right is None:
        right = len(arr) - 1

    if left >= right:
        return

    mid = (left + right) // 2

    merge_sort(arr, left, mid)
    merge_sort(arr, mid + 1, right)

    merge(arr, left, mid, right)