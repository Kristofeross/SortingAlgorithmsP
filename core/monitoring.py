import time
import psutil


try:
    import resource
    HAS_RESOURCE = True
except ImportError:
    # Windows does not have a 'resource' module
    resource = None
    HAS_RESOURCE = False


def get_processes(proc):
    try:
        return [proc] + proc.children(recursive=True)
    except psutil.NoSuchProcess:
        return []


def get_memory_mb(process):
    try:
        full_info = process.memory_full_info()
        if hasattr(full_info, "pss"):
            return full_info.pss / (1024 ** 2)
    except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError, ValueError):
        pass

    # Fallback: Regular RSS
    return process.memory_info().rss / (1024 ** 2)


def measure_usage(proc, interval, cpu_samples, mem_samples, states, stop_flag):
    previous_wall_time = time.perf_counter()
    previous_cpu_times = {}

    while not stop_flag.is_set():
        current_wall_time = time.perf_counter()
        delta_wall = current_wall_time - previous_wall_time

        total_cpu_percent = 0.0
        total_mem = 0.0

        processes = get_processes(proc)


        for process in processes:
            try:
                cpu_times = process.cpu_times()

                current_cpu_time = cpu_times.user + cpu_times.system
                mem = get_memory_mb(process)
                total_mem += mem

                if process.pid in previous_cpu_times:
                    delta_cpu = current_cpu_time - previous_cpu_times[process.pid]

                    if delta_wall > 0:
                        cpu_percent = (delta_cpu / delta_wall) * 100

                    else:
                        cpu_percent = 0.0

                else:
                    cpu_percent = 0.0

                previous_cpu_times[process.pid] = current_cpu_time
                total_cpu_percent += cpu_percent


            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue


        active_pids = {p.pid for p in processes}

        for pid in list(previous_cpu_times.keys()):
            if pid not in active_pids:
                del previous_cpu_times[pid]


        cpu_samples.append(total_cpu_percent)
        mem_samples.append(total_mem)

        state = "idle" if total_cpu_percent < 20 else "active"

        states.append(state)
        previous_wall_time = current_wall_time
        time.sleep(interval)


def get_exact_children_cpu_time():
    if not HAS_RESOURCE:
        return None

    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime