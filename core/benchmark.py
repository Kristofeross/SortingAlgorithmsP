import time
import cProfile
import pstats
import io
import psutil
import os
import threading
import statistics

from .monitoring import measure_usage

def profile_function(func, *args, label="Profilowanie", sort_by="cumulative", repeat=4, sample_interval=0.05, sequential_time=None, cores=1):
    times = []
    cpu_results = []
    mem_results = []

    pr = cProfile.Profile()

    for run in range(repeat + 1):
        main_proc = psutil.Process(os.getpid())
        cpu_samples = []
        mem_samples = []
        states = []
        stop_flag = threading.Event()

        measurer = threading.Thread(
            target=measure_usage,
            args=(
                main_proc,
                sample_interval,
                cpu_samples,
                mem_samples,
                states,
                stop_flag
            )
        )

        if run == 0:
            pr.enable()

        start_time = time.perf_counter()
        measurer.start()

        result = func(*args)

        stop_flag.set()
        measurer.join()
        end_time = time.perf_counter()

        if run == 0:
            pr.disable()

        execution_time = end_time - start_time

        avg_cpu_run = (
            sum(cpu_samples) / len(cpu_samples)
            if cpu_samples else 0
        )

        avg_mem_run = (
            sum(mem_samples) / len(mem_samples)
            if mem_samples else 0
        )

        active_time = states.count("active") * sample_interval
        idle_time = states.count("idle") * sample_interval

        print(
            f"Run {run + 1}: "
            f"CPU avg={avg_cpu_run:.2f}%, "
            f"RAM avg={avg_mem_run:.2f} MB, "
            f"Active time={active_time:.2f}s, "
            f"Idle time={idle_time:.2f}s, "
            f"Total time={execution_time:.4f}s"
        )

        if run == 0:
            print("Pierwsze uruchomienie pominięte\n")
            continue

        times.append(execution_time)
        cpu_results.append(avg_cpu_run)
        mem_results.append(avg_mem_run)

    avg_time = statistics.mean(times)
    median_time = statistics.median(times)

    std_time = (
        statistics.stdev(times)
        if len(times) > 1 else 0
    )

    avg_cpu = statistics.mean(cpu_results)
    avg_mem = statistics.mean(mem_results)

    # Speedup i Efficiency
    speedup = None
    efficiency = None

    if sequential_time is not None:
        speedup = sequential_time / avg_time
        if cores > 0:
            efficiency = speedup / cores

    s = io.StringIO()

    ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
    ps.print_stats(20)

    print(f"\n--- {label} ---")
    print(f"Średni czas wykonania: {avg_time:.6f} s")
    print(f"Mediana czasu wykonania: {median_time:.6f} s")
    print(f"Odchylenie standardowe: {std_time:.6f} s")
    print(f"Średnie obciążenie CPU: {avg_cpu:.2f}%")
    print(f"Średnie użycie RAM: {avg_mem:.2f} MB")

    if speedup is not None:
        print(f"Speedup: {speedup:.4f}")

    if efficiency is not None:
        print(f"Efficiency: {efficiency:.4f}")

    print("\n=== cProfile ===")
    print(s.getvalue())

    return {
        "result": result,
        "avg_time": avg_time,
        "median_time": median_time,
        "std_time": std_time,
        "avg_cpu": avg_cpu,
        "avg_mem": avg_mem,
        "speedup": speedup,
        "efficiency": efficiency
    }
