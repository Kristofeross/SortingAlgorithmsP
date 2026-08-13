import time
import cProfile
import pstats
import io
import multiprocessing as mp
import threading
import statistics
import psutil
import os
import subprocess
import shutil
import platform

from core.monitoring import measure_usage, get_exact_children_cpu_time, HAS_RESOURCE


DEFAULT_TIMEOUT = 900
DEFAULT_SAMPLE_INTERVAL = 0.05
PYSPY_PATH = shutil.which("py-spy")

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

        cpu_time_before = get_exact_children_cpu_time()

        start_time = time.perf_counter()
        monitor_thread.start()

        result = execute_algorithm(func, args)

        end_time = time.perf_counter()
        stop_flag.set()
        monitor_thread.join()

        execution_time = end_time - start_time

        cpu_time_after = get_exact_children_cpu_time()

        exact_children_cpu_time = None
        if cpu_time_before is not None and cpu_time_after is not None:
            exact_children_cpu_time = cpu_time_after - cpu_time_before

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

        sample_count = len(cpu_samples)

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
            "error_message": error_message,
            "exact_children_cpu_time": exact_children_cpu_time,
            "sample_count": sample_count,
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


def launch_pyspy(pid, output_path, timeout):
    if PYSPY_PATH is None:
        print("Uwaga: py-spy nie jest zainstalowane | profilowanie subprocesów pominięte")
        return None

    command = [
        PYSPY_PATH, "record",
        "--pid", str(pid),
        "--subprocesses",
        "--nonblocking",
        "--output", output_path,
        "--format", "flamegraph",
        "--rate", "100",
        "--duration", str(max(int(timeout), 1)),
    ]

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return process
    except Exception as exception:
        print(f"Uwaga: nie udało się uruchomić py-spy: {exception}")
        return None


def stop_pyspy(pyspy_process, output_path, elapsed_hint=None):
    if pyspy_process is None:
        return

    if pyspy_process.poll() is None:
        try:
            pyspy_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pyspy_process.terminate()
            try:
                pyspy_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pyspy_process.kill()
                pyspy_process.wait()

    stdout, stderr = pyspy_process.communicate() if pyspy_process.stdout else (None, None)
    return_code = pyspy_process.returncode

    stdout_text = stdout.decode(errors="ignore").strip() if stdout else ""
    stderr_text = stderr.decode(errors="ignore").strip() if stderr else ""

    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        print(f"Flamegraph subprocesów zapisany: {output_path}")
        return

    print(f"Uwaga: py-spy nie zapisał flamegraph plik pusty/brak. Kod wyjścia py-spy: {return_code}")


    if elapsed_hint is not None and elapsed_hint < 0.3:
        print(
            f"Prawdopodobna przyczyna: proces zakończył się bardzo szybko ~{elapsed_hint:.3f}s, py-spy najpewniej nie zdążyło się dołączyć"
        )
    else:
        if platform.system() == "Windows":
            permission_hint = ("Możliwa przyczyna na Windowsie: brak uprawnień administratora")
        else:
            permission_hint = ("Możliwa przyczyna na Linuksie: brak uprawnień ptrace")
        print(f"  {permission_hint}")

    if stderr_text:
        print(f"  py-spy stderr: {stderr_text[:300]}")
    if stdout_text:
        print(f"  py-spy stdout: {stdout_text[:300]}")


def run_single_benchmark(func, args, timeout=DEFAULT_TIMEOUT,
        sample_interval=DEFAULT_SAMPLE_INTERVAL, profile_enabled=False, sort_by="cumulative", pyspy_output=None):

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

    pyspy_process = None
    pyspy_launch_time = None
    if profile_enabled and pyspy_output is not None:
        pyspy_launch_time = time.perf_counter()
        pyspy_process = launch_pyspy(worker.pid, pyspy_output, timeout)

    worker.join(timeout)

    if pyspy_process is not None:
        elapsed_hint = (
            time.perf_counter() - pyspy_launch_time
            if pyspy_launch_time is not None else None
        )
        stop_pyspy(pyspy_process, pyspy_output, elapsed_hint=elapsed_hint)

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


def profile_function(func, *args, label="Profilowanie", sort_by="cumulative", repeat=10, sample_interval=DEFAULT_SAMPLE_INTERVAL,
        sequential_time=None, cores=1, timeout=DEFAULT_TIMEOUT, profile_subprocesses=True, profile_dir="profiles"):

    times = []
    cpu_results = []
    mem_results = []
    max_mem_results = []
    exact_cpu_results = []
    sample_counts = []

    correctness = "CORRECT"
    profile_output = ""
    status = "OK"
    error_message = None

    pyspy_output = None
    if profile_subprocesses and PYSPY_PATH is not None:
        os.makedirs(profile_dir, exist_ok=True)
        safe_label = label.replace(" ", "_").replace("/", "_")
        pyspy_output = os.path.join(profile_dir, f"{safe_label}.svg")

    for run in range(repeat + 1):
        run_data = run_single_benchmark(
            func=func,
            args=args,
            timeout=timeout,
            sample_interval=sample_interval,
            profile_enabled=(run == 0),
            sort_by=sort_by,
            pyspy_output=(pyspy_output if run == 0 else None)
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

        exact_cpu_run = run_data.get("exact_children_cpu_time")
        sample_count_run = run_data.get("sample_count", 0)

        exact_cpu_str = f"{exact_cpu_run:.4f}s" if exact_cpu_run is not None else "N/A (Windows)"
        low_confidence_note = " [UWAGA: mało próbek]" if sample_count_run < 3 else ""

        # print(
        #     f"Run {run + 1}: "
        #     f"CPU avg={avg_cpu_run:.2f}%, "
        #     f"RAM avg={avg_mem_run:.2f} MB, "
        #     f"RAM max={max_mem_run:.2f} MB, "
        #     f"Active time={active_time:.2f}s, "
        #     f"Idle time={idle_time:.2f}s, "
        #     f"Total time={execution_time:.4f}s, "
        #     f"Próbki={sample_count_run}{low_confidence_note}"
        # )

        if run == 0:
            # print("Pierwsze uruchomienie pominięte\n")
            profile_output = run_data["profile"]
            continue

        times.append(execution_time)
        cpu_results.append(avg_cpu_run)
        mem_results.append(avg_mem_run)
        max_mem_results.append(max_mem_run)
        sample_counts.append(sample_count_run)
        if exact_cpu_run is not None:
            exact_cpu_results.append(exact_cpu_run)

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
            "error_message": error_message,
            "avg_exact_cpu_time": None,
            "min_sample_count": None,
        }

    avg_time = statistics.mean(times) if times else None
    median_time = statistics.median(times) if times else None
    std_time = statistics.stdev(times) if len(times) > 1 else 0

    avg_cpu = statistics.mean(cpu_results) if cpu_results else None
    avg_mem = statistics.mean(mem_results) if mem_results else None
    max_mem = max(max_mem_results) if max_mem_results else None

    avg_exact_cpu_time = statistics.mean(exact_cpu_results) if exact_cpu_results else None
    min_sample_count = min(sample_counts) if sample_counts else None

    speedup = None
    efficiency = None

    if sequential_time is not None and avg_time is not None and avg_time > 0:
        speedup = sequential_time / avg_time
        if cores > 0:
            efficiency = speedup / cores

    # print(f"\n--- {label} ---")
    # print(f"Status: {status}")
    # print(f"Środowisko: {'resource dostępne (Linux/macOS)' if HAS_RESOURCE else 'resource niedostępne (Windows) - brak dokładnego CPU'}")
    # print(f"Średni czas wykonania: {avg_time:.6f} s")
    # print(f"Mediana czasu wykonania: {median_time:.6f} s")
    # print(f"Odchylenie standardowe: {std_time:.6f} s")
    # print(f"Średnie obciążenie CPU (próbkowane): {avg_cpu:.2f}%")
    # if avg_exact_cpu_time is not None:
    #     print(f"Średni dokładny czas CPU (getrusage): {avg_exact_cpu_time:.4f} s")
    # print(f"Średnie użycie RAM: {avg_mem:.2f} MB")
    # print(f"Maksymalne użycie RAM: {max_mem:.2f} MB")
    # if min_sample_count is not None and min_sample_count < 3:
    #     print(f"UWAGA: minimalna liczba próbek w przebiegach = {min_sample_count} - statystyki CPU/RAM mogą być niewiarygodne dla tego przypadku")
    #
    # if error_message is not None:
    #     print(f"Błąd: {error_message}")
    # if speedup is not None:
    #     print(f"Speedup: {speedup:.4f}")
    # if efficiency is not None:
    #     print(f"Efficiency: {efficiency:.4f}")

    # print("\n--- cProfile ---")
    # print(profile_output)

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
        "error_message": error_message,
        "avg_exact_cpu_time": avg_exact_cpu_time,
        "min_sample_count": min_sample_count,
    }