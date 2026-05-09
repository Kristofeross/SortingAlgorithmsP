from .utils import partition

def quicksort(arr):
    if len(arr) <= 1:
        return arr
    left, middle, right = partition(arr)

    return quicksort(left) + middle + quicksort(right)