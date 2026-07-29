IMPLEMENTATION_VERSION = "shared_memory"

if IMPLEMENTATION_VERSION == "queue":
    from algorithms.queue.quicksort.sequential import quicksort
    from algorithms.queue.quicksort.parallel import parallel_quicksort
    from algorithms.queue.mergesort.sequential import merge_sort
    from algorithms.queue.mergesort.parallel import parallel_merge_sort
    from algorithms.queue.bucketsort.sequential import bucket_sort
    from algorithms.queue.bucketsort.parallel import parallel_bucket_sort
    from algorithms.queue.samplesort.sequential import sample_sort
    from algorithms.queue.samplesort.parallel import parallel_sample_sort
elif IMPLEMENTATION_VERSION == "shared_memory":
    from algorithms.shared_memory.quicksort.sequential import quicksort
    from algorithms.shared_memory.quicksort.parallel import parallel_quicksort
    from algorithms.shared_memory.mergesort.sequential import merge_sort
    from algorithms.shared_memory.mergesort.parallel import parallel_merge_sort
    from algorithms.shared_memory.bucketsort.sequential import bucket_sort
    from algorithms.shared_memory.bucketsort.parallel import parallel_bucket_sort
    from algorithms.shared_memory.samplesort.sequential import sample_sort
    from algorithms.shared_memory.samplesort.parallel import parallel_sample_sort
elif IMPLEMENTATION_VERSION == "cpp":
    from algorithms.queue.quicksort.sequential import quicksort
else:
    raise ValueError(f"Nieznana wersja implementacji: {IMPLEMENTATION_VERSION}")

# available algorithms
ALGORITHMS = {
    "1": {
        "name": "Quick Sort",
        "sequential": quicksort,
        "parallel": parallel_quicksort
    },
    "2": {
        "name": "Merge Sort",
        "sequential": merge_sort,
        "parallel": parallel_merge_sort
    },
    "3": {
        "name": "Bucket Sort",
        "sequential": bucket_sort,
        "parallel": parallel_bucket_sort
    },
    "4": {
        "name": "Sample Sort",
        "sequential": sample_sort,
        "parallel": parallel_sample_sort
    },
}

# available tables of data
DATA_TABLES = {
    "1": ("random_int", "Losowe liczby całkowite"),
    "2": ("random_float", "Losowe liczby zmiennoprzecinkowe"),
    "3": ("duplicates_int", "Całkowite liczby z duplikatami"),
    "4": ("duplicates_float", "Zmienne liczby z duplikatami"),
    "5": ("part_sorted20_int", "20% posortowanych liczb całkowitych"),
    "6": ("part_sorted20_float", "20% posortowanych liczb zmiennoprzecinkowych"),
    "7": ("part_sorted40_int", "40% posortowanych liczb całkowitych"),
    "8": ("part_sorted40_float", "40% posortowanych liczb zmiennoprzecinkowych"),
    "9": ("part_sorted60_int", "60% posortowanych liczb całkowitych"),
    "10": ("part_sorted60_float", "60% posortowanych liczb zmiennoprzecinkowych"),
    "11": ("part_sorted80_int", "80% posortowanych liczb całkowitych"),
    "12": ("part_sorted80_float", "80% posortowanych liczb zmiennoprzecinkowych")
}

# sizes of data
DATA_SIZES = {
    "1": 1000,
    "2": 10000,
    "3": 100000,
    "4": 1000000
}