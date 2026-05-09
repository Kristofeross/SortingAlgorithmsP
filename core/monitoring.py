import time
import psutil



def get_total_usage(proc):
    try:
        children = proc.children(recursive=True)
    except psutil.NoSuchProcess:
        return 0.0, 0.0

    total_cpu = proc.cpu_percent(interval=0.05)
    total_mem = proc.memory_info().rss / (1024 ** 2)

    for child in children:
        try:
            total_cpu += child.cpu_percent(interval=0.0)
            total_mem += child.memory_info().rss / (1024 ** 2)
        except psutil.NoSuchProcess:
            pass

    return total_cpu, total_mem


def measure_usage(main_proc, interval, cpu_samples, mem_samples, states, stop_flag):
    while not stop_flag.is_set():
        cpu_now, mem_now = get_total_usage(main_proc)
        cpu_samples.append(cpu_now)
        mem_samples.append(mem_now)

        state = "idle" if cpu_now < 5 else "active"  # Próg 5% CPU
        states.append(state)

        time.sleep(interval)