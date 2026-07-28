from algorithms.shared_memory.quicksort.utils import partition
import random
from algorithms.shared_memory.quicksort.utils import (
    create_shared_array,
    attach_shared_array,
    destroy_shared_memory
)

def quicksort(arr, low=0, high=None):
    if high is None:
        high = len(arr) - 1

    if low >= high:
        return

    index = partition(arr, low, high)

    quicksort(arr, low, index - 1)
    quicksort(arr, index, high)


# def main():
#     data = list(range(10000))
#     random.shuffle(data)
#
#     expected = sorted(data)
#
#     quicksort(data)
#
#     print(data == expected)
#
#     if data != expected:
#         for i in range(len(data) - 1):
#             if data[i] > data[i + 1]:
#                 print("Błąd na pozycji:", i)
#                 print(data[i-3:i+4])
#                 break
#
#
# if __name__ == "__main__":
#     main()