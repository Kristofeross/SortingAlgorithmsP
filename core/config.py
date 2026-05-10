from algorithms.quicksort.sequential import quicksort
from algorithms.quicksort.parallel_v1 import parallel_quicksort
from algorithms.mergesort.parallel import merge_sort
from algorithms.mergesort.parallel import parallel_merge_sort
from algorithms.bucketsort.sequential import bucket_sort
from algorithms.bucketsort.parallel import parallel_bucket_sort
from algorithms.samplesort.sequential import sample_sort
from algorithms.samplesort.parallel import parallel_sample_sort


# Dostępne algorytmy
ALGORITHMS = {
    "1": {
        "name": "Parallel QuickSort",
        "sequential": quicksort,
        "parallel": parallel_quicksort
    },
    "2": {
        "name": "Parallel MergeSort",
        "sequential": merge_sort,
        "parallel": parallel_merge_sort
    },
    "3": {
        "name": "Parallel BucketSort",
        "sequential": bucket_sort,
        "parallel": parallel_bucket_sort
    },
    "4": {
        "name": "Sample Sort",
        "sequential": sample_sort,
        "parallel": parallel_sample_sort
    },
    # "5": {
    #     "name": "Odd-Even Transposition Sort",
    #     "sequential": ,
    #     "parallel":
    # }
}

# Dostępne tabele danych
DATA_TABLES = {
    "1": ("random_int", "Losowe liczby całkowite"),
    "2": ("random_float", "Losowe liczby zmiennoprzecinkowe"),
    "3": ("duplicates_int", "Całkowite liczby z duplikatami"),
    "4": ("duplicates_float", "Zmienne liczby z duplikatami"),
    "5": ("part_sorted_int", "Częściowo posortowane liczby całkowite"),
    "6": ("part_sorted_float", "Częściowo posortowane liczby zmiennoprzecinkowe")
}

# Rozmiary danych
DATA_SIZES = {
    "1": 1000,
    "2": 10000,
    "3": 100000,
    "4": 1000000
}