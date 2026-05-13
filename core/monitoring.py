import time
import psutil

def get_total_usage(proc):
    total_cpu = 0.0
    total_mem = 0.0

    try:
        processes = [proc] + proc.children(recursive=True)
    except psutil.NoSuchProcess:
        return 0.0, 0.0

    for p in processes:
        try:
            # CPU
            total_cpu += p.cpu_percent(interval=None)
            # RAM
            total_mem += p.memory_info().rss / (1024 ** 2)

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    return total_cpu, total_mem


def measure_usage(main_proc, interval, cpu_samples, mem_samples, states, stop_flag):
    # Inicjalization counter CPU
    processes = [main_proc] + main_proc.children(recursive=True)

    for p in processes:
        try:
            p.cpu_percent(interval=None)
        except:
            pass

    while not stop_flag.is_set():
        cpu_now, mem_now = get_total_usage(main_proc)

        cpu_samples.append(cpu_now)
        mem_samples.append(mem_now)

        state = "idle" if cpu_now < 20 else "active"
#       state = "idle" if cpu_now < 5 else "active"
        states.append(state)

        time.sleep(interval)