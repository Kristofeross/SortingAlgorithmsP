def insertion_sort(arr):
    for i in range(1, len(arr)):
        key = arr[i]
        j = i - 1

        while j >= 0 and arr[j] > key:
            arr[j + 1] = arr[j]
            j -= 1

        arr[j + 1] = key

    return arr


def distribute_to_buckets(arr, num_buckets):
    if len(arr) == 0:
        return []

    min_val = min(arr)
    max_val = max(arr)

    if min_val == max_val:
        buckets = [[] for _ in range(num_buckets)]
        buckets[0] = arr

        return buckets

    bucket_range = (max_val - min_val + 1) / num_buckets
    buckets = [[] for _ in range(num_buckets)]

    for num in arr:
        index = min(num_buckets - 1, int((num - min_val) / bucket_range))
        buckets[index].append(num)

    return buckets