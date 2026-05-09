import time
import cProfile
import pstats
import io
import psutil
import os
import threading

from .monitoring import measure_usage

def profile_function(func, *args, label="Profilowanie", sort_by="cumulative", repeat=8, sample_interval=0.1):
    total_time = 0.0
    total_cpu = 0.0
    total_mem = 0.0

    pr = cProfile.Profile()

    for run in range(repeat):
        main_proc = psutil.Process(os.getpid())
        main_proc.cpu_percent(interval=None)
        for child in main_proc.children(recursive=True):
            child.cpu_percent(interval=None)

        cpu_samples = []
        mem_samples = []
        states = []
        stop_flag = threading.Event()
        measurer = threading.Thread(target=measure_usage,
                                    args=(main_proc, sample_interval, cpu_samples, mem_samples, states, stop_flag))

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

        avg_cpu_run = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
        avg_mem_run = sum(mem_samples) / len(mem_samples) if mem_samples else 0

        active_time = states.count("active") * sample_interval
        idle_time = states.count("idle") * sample_interval

        print(f"Run {run + 1}: CPU avg={avg_cpu_run:.2f}%, RAM avg={avg_mem_run:.2f} MB, "
              f"Active time={active_time:.2f}s, Idle time={idle_time:.2f}s, Total time={end_time - start_time:.4f}s")

        # Save to file
        # filename = f"profile_{label.replace(' ', '_')}_run{run + 1}.txt"
        # with open(filename, 'w') as f:
        #     timestamp = start_time
        #     for i in range(len(cpu_samples)):
        #         line = f"timestamp: {timestamp:.2f}, cpu_percent: {cpu_samples[i]:.2f}, ram_MB: {mem_samples[i]:.2f}, state: {states[i]}\n"
        #         f.write(line)
        #         timestamp += sample_interval

        total_time += (end_time - start_time)
        total_cpu += avg_cpu_run
        total_mem += avg_mem_run

    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
    ps.print_stats(20)

    print(f"\n--- {label} ---")
    print(f"Średni czas wykonania ({repeat} prób): {total_time / repeat:.6f} s")
    print(f"Średnie obciążenie CPU (%): {total_cpu / repeat:.2f}")
    print(f"Średnie użycie RAM (MB): {total_mem / repeat:.2f}")
    print(s.getvalue())

    return result