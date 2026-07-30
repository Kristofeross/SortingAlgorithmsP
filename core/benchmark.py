import time
import cProfile
import pstats
import io
import multiprocessing as mp
import threading
import statistics
import psutil
import os

from core.monitoring import measure_usage


DEFAULT_TIMEOUT = 900
# DEFAULT_SAMPLE_INTERVAL = 0.1
DEFAULT_SAMPLE_INTERVAL = 0.05


def execute_algorithm(func, args):
    return func(*args)


def benchmark_worker(func, args, result_queue, sample_interval, profile_enabled, sort_by):
    cpu_samples = []
    mem_samples = []
    states = []

    stop_flag = threading.Event()

    worker_process = psutil.Process(os.getpid())

    monitor_thread = threading.Thread(
        target=measure_usage,
        args=(worker_process, sample_interval, cpu_samples, mem_samples, states, stop_flag)
    )

    profiler = cProfile.Profile()

    try:
        if profile_enabled:
            profiler.enable()

        original = list(args[0])

        start_time = time.perf_counter()
        monitor_thread.start()

        result = execute_algorithm(func, args)

        end_time = time.perf_counter()
        stop_flag.set()
        monitor_thread.join()

        execution_time = end_time - start_time

        if profile_enabled:
            profiler.disable()

        expected = sorted(original)

        if result is None:
            correctness = "CORRECT" if args[0] == expected else "INCORRECT"
        else:
            correctness = "CORRECT" if result == expected else "INCORRECT"

        error_message = None

        if correctness == "INCORRECT":
            error_message = "Algorytm zwrócił niepoprawnie posortowane dane."

        avg_cpu = (
            statistics.mean(cpu_samples)
            if cpu_samples else 0
        )

        avg_mem = (
            statistics.mean(mem_samples)
            if mem_samples else 0
        )

        max_mem = (
            max(mem_samples)
            if mem_samples else 0
        )

        active_time = (
            states.count("active")
            * sample_interval
        )

        idle_time = (
            states.count("idle")
            * sample_interval
        )

        profile_output = ""

        if profile_enabled:
            profile_stream = io.StringIO()

            profile_stats = pstats.Stats(
                profiler,
                stream=profile_stream
            ).sort_stats(sort_by)

            profile_stats.print_stats(20)

            profile_output = profile_stream.getvalue()

        result_queue.put({
            "status": "OK",
            "execution_time": execution_time,
            "avg_cpu": avg_cpu,
            "avg_mem": avg_mem,
            "max_mem": max_mem,
            "active_time": active_time,
            "idle_time": idle_time,
            "correctness": correctness,
            "profile": profile_output,
            "error_message": error_message
        })

    except Exception as exception:
        stop_flag.set()

        if monitor_thread.is_alive():
            monitor_thread.join()

        result_queue.put({
            "status": "ERROR",
            "correctness": "UNKNOWN",
            "error_message": str(exception)
        })


def run_single_benchmark(func, args, timeout=DEFAULT_TIMEOUT,
        sample_interval=DEFAULT_SAMPLE_INTERVAL, profile_enabled=False, sort_by="cumulative"):

    result_queue = mp.Queue()

    worker = mp.Process(
        target=benchmark_worker,
        args=(
            func,
            args,
            result_queue,
            sample_interval,
            profile_enabled,
            sort_by
        )
    )

    worker.start()
    worker.join(timeout)


    if worker.is_alive():
        worker.terminate()
        worker.join()

        return {
            "status": "TIMEOUT",
            "correctness": "UNKNOWN",
            "error_message": f"Przekroczono limit czasu ({timeout} s)",
        }

    if result_queue.empty():
        return {
            "status": "ERROR",
            "correctness": "UNKNOWN",
            "error_message": "Proces zakończył się bez zwrócenia wyniku.",
        }

    result = result_queue.get()

    return result


def profile_function(func, *args, label="Profilowanie", sort_by="cumulative", repeat=2,
        sample_interval=DEFAULT_SAMPLE_INTERVAL, sequential_time=None, cores=1, timeout=DEFAULT_TIMEOUT):
    times = []
    cpu_results = []
    mem_results = []
    max_mem_results = []

    correctness = "CORRECT"
    profile_output = ""
    status = "OK"
    error_message = None

    for run in range(repeat + 1):
        run_data = run_single_benchmark(
            func=func,
            args=args,
            timeout=timeout,
            sample_interval=sample_interval,
            profile_enabled=(run == 0),
            sort_by=sort_by
        )

        run_status = run_data["status"]

        if run_status != "OK":
            status = run_data["status"]
            error_message = run_data["error_message"]
            print( f"Run {run + 1}: {run_status} - {error_message}" )
            break

        if run_data["correctness"] != "CORRECT":
            correctness = "INCORRECT"
            error_message = run_data["error_message"]

        execution_time = run_data["execution_time"]
        avg_cpu_run = run_data["avg_cpu"]
        avg_mem_run = run_data["avg_mem"]
        max_mem_run = run_data["max_mem"]
        active_time = run_data["active_time"]
        idle_time = run_data["idle_time"]

        print(
            f"Run {run + 1}: "
            f"CPU avg={avg_cpu_run:.2f}%, "
            f"RAM avg={avg_mem_run:.2f} MB, "
            f"RAM max={max_mem_run:.2f} MB, "
            f"Active time={active_time:.2f}s, "
            f"Idle time={idle_time:.2f}s, "
            f"Total time={execution_time:.4f}s"
        )

        if run == 0:
            print("Pierwsze uruchomienie pominięte\n")
            profile_output = run_data["profile"]
            continue

        times.append(execution_time)
        cpu_results.append(avg_cpu_run)
        mem_results.append(avg_mem_run)
        max_mem_results.append(max_mem_run)

    # Benchmark finished with error
    if status != "OK":
        print(f"\n--- {label} ---")
        print(f"Status: {status}")
        print(f"Błąd: {error_message}")

        return {
            "avg_time": None,
            "median_time": None,
            "std_time": None,
            "avg_cpu": None,
            "avg_mem": None,
            "max_mem": None,
            "speedup": None,
            "efficiency": None,
            "status": status,
            "correctness": "UNKNOWN",
            "error_message": error_message
        }

    # Calculating statistics
    avg_time = statistics.mean(times) if times else None
    median_time = statistics.median(times) if times else None
    std_time = statistics.stdev(times) if len(times) > 1 else 0

    avg_cpu = statistics.mean(cpu_results) if cpu_results else None
    avg_mem = statistics.mean(mem_results) if mem_results else None
    max_mem = max(max_mem_results) if max_mem_results else None

    # Speedup i Efficiency
    speedup = None
    efficiency = None

    if sequential_time is not None and avg_time is not None and avg_time > 0:
        speedup = sequential_time / avg_time
        if cores > 0:
            efficiency = speedup / cores

    # Summary
    print(f"\n--- {label} ---")
    print(f"Status: {status}")
    print(f"Średni czas wykonania: {avg_time:.6f} s")
    print(f"Mediana czasu wykonania: {median_time:.6f} s")
    print(f"Odchylenie standardowe: {std_time:.6f} s")
    print(f"Średnie obciążenie CPU: {avg_cpu:.2f}%")
    print(f"Średnie użycie RAM: {avg_mem:.2f} MB")
    print(f"Maksymalne użycie RAM: {max_mem:.2f} MB")

    if error_message is not None:
        print(f"Błąd: {error_message}")
    if speedup is not None:
        print(f"Speedup: {speedup:.4f}")
    if efficiency is not None:
        print(f"Efficiency: {efficiency:.4f}")
    print("\n--- cProfile ---")
    print(profile_output)

    return {
        "avg_time": avg_time,
        "median_time": median_time,
        "std_time": std_time,
        "avg_cpu": avg_cpu,
        "avg_mem": avg_mem,
        "max_mem": max_mem,
        "speedup": speedup,
        "efficiency": efficiency,
        "status": "OK",
        "correctness": correctness,
        "error_message": error_message
    }