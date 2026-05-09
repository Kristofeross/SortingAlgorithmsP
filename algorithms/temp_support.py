# functions found in different versions of parallel quicksort

import multiprocessing as mp
import time
import sqlite3
import cProfile
import pstats
import io

import psutil
import os
import threading

import datetime

# def get_data_from_db(table_name, set_size, db_path="../dane.db"):
#     conn = sqlite3.connect(db_path)
#     cursor = conn.cursor()
#     cursor.execute(f"SELECT value FROM {table_name} WHERE set_size = ?", (set_size,))
#     rows = cursor.fetchall()
#     conn.close()
#
#     return [row[0] for row in rows]

# def partition(arr):
#     pivot = arr[len(arr) // 2]
#     left = [x for x in arr if x < pivot]
#     middle = [x for x in arr if x == pivot]
#     right = [x for x in arr if x > pivot]
#
#     return left, middle, right

# def quicksort(arr):
#     if len(arr) <= 1:
#         return arr
#     left, middle, right = partition(arr)
#
#     return quicksort(left) + middle + quicksort(right)

# def test_get_data():
#     table_name = "random_int"
#     set_size = 1000000
#     data = get_data_from_db(table_name, set_size)
#     return data

# def test_cores():
#     available_cores = mp.cpu_count()
#     print(f"Dostępne rdzenie: {available_cores} | Wprowadź liczbę rdzeni:")
#     try:
#         cores = int(input())
#         # cores = 8
#     except:
#         cores = 1
#     if cores > available_cores:
#         print(
#             f"Podana liczba rdzeni {cores} jest większa niż dostępne {available_cores}. Ustawiam na {available_cores}.")
#         cores = available_cores
#     elif cores < 1:
#         print("Minimalna liczba rdzeni to 1. Ustawiam na 1.")
#         cores = 1
#
#     # max_depth = cores.bit_length()  # lub np. int(math.log2(cores))
#     # max_depth = int(math.log2(cores))
#
#     return cores

# def sequence_quicksot(data):
#     start = time.perf_counter()
#     sorted_seq = quicksort(data)
#     end = time.perf_counter()
#     print(f"\nPo sortowaniu sekwencyjnym (czas): {end - start:.6f} s")
#
#     return sorted_seq

# def get_total_usage(proc):
#     try:
#         children = proc.children(recursive=True)
#     except psutil.NoSuchProcess:
#         return 0.0, 0.0
#
#     total_cpu = proc.cpu_percent(interval=0.05)
#     total_mem = proc.memory_info().rss / (1024 ** 2)
#
#     for child in children:
#         try:
#             total_cpu += child.cpu_percent(interval=0.0)
#             total_mem += child.memory_info().rss / (1024 ** 2)
#         except psutil.NoSuchProcess:
#             pass
#
#     return total_cpu, total_mem


# def measure_usage(main_proc, interval, cpu_samples, mem_samples, states, stop_flag):
#     while not stop_flag.is_set():
#         cpu_now, mem_now = get_total_usage(main_proc)
#         cpu_samples.append(cpu_now)
#         mem_samples.append(mem_now)
#
#         state = "idle" if cpu_now < 5 else "active"  # Próg 5% CPU
#         states.append(state)
#
#         time.sleep(interval)


# def profile_function(func, *args, label="Profilowanie", sort_by="cumulative", repeat=8, sample_interval=0.1):
#     total_time = 0.0
#     total_cpu = 0.0
#     total_mem = 0.0
#
#     pr = cProfile.Profile()
#
#     for run in range(repeat):
#         main_proc = psutil.Process(os.getpid())
#         main_proc.cpu_percent(interval=None)
#         for child in main_proc.children(recursive=True):
#             child.cpu_percent(interval=None)
#
#         cpu_samples = []
#         mem_samples = []
#         states = []
#         stop_flag = threading.Event()
#         measurer = threading.Thread(target=measure_usage,
#                                     args=(main_proc, sample_interval, cpu_samples, mem_samples, states, stop_flag))
#
#         if run == 0:
#             pr.enable()
#
#         start_time = time.perf_counter()
#         measurer.start()
#
#         result = func(*args)
#
#         stop_flag.set()
#         measurer.join()
#         end_time = time.perf_counter()
#
#         if run == 0:
#             pr.disable()
#
#         avg_cpu_run = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
#         avg_mem_run = sum(mem_samples) / len(mem_samples) if mem_samples else 0
#
#         active_time = states.count("active") * sample_interval
#         idle_time = states.count("idle") * sample_interval
#
#         print(f"Run {run + 1}: CPU avg={avg_cpu_run:.2f}%, RAM avg={avg_mem_run:.2f} MB, "
#               f"Active time={active_time:.2f}s, Idle time={idle_time:.2f}s, Total time={end_time - start_time:.4f}s")
#
#         # Save to file
#         # filename = f"profile_{label.replace(' ', '_')}_run{run + 1}.txt"
#         # with open(filename, 'w') as f:
#         #     timestamp = start_time
#         #     for i in range(len(cpu_samples)):
#         #         line = f"timestamp: {timestamp:.2f}, cpu_percent: {cpu_samples[i]:.2f}, ram_MB: {mem_samples[i]:.2f}, state: {states[i]}\n"
#         #         f.write(line)
#         #         timestamp += sample_interval
#
#         total_time += (end_time - start_time)
#         total_cpu += avg_cpu_run
#         total_mem += avg_mem_run
#
#     s = io.StringIO()
#     ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
#     ps.print_stats(20)
#
#     print(f"\n--- {label} ---")
#     print(f"Średni czas wykonania ({repeat} prób): {total_time / repeat:.6f} s")
#     print(f"Średnie obciążenie CPU (%): {total_cpu / repeat:.2f}")
#     print(f"Średnie użycie RAM (MB): {total_mem / repeat:.2f}")
#     print(s.getvalue())
#
#     return result