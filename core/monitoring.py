import time
import psutil


def get_processes(proc):
    try:
        return [proc] + proc.children(recursive=True)
    except psutil.NoSuchProcess:
        return []


def measure_usage(proc, interval, cpu_samples, mem_samples, states, stop_flag):
    previous_wall_time = time.perf_counter()
    previous_cpu_times = {}

    while not stop_flag.is_set():
        current_wall_time = time.perf_counter()
        delta_wall = current_wall_time - previous_wall_time

        total_cpu_percent = 0.0
        total_mem = 0.0

        processes = get_processes(proc)

        # print("\n--- MONITOR PROCESÓW ---")

        for process in processes:
            try:
                cpu_times = process.cpu_times()

                current_cpu_time = cpu_times.user + cpu_times.system
                mem = process.memory_info().rss / (1024 ** 2)
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

                # print(
                #     f"PID: {process.pid} | "
                #     f"Nazwa: {process.name()} | "
                #     f"CPU: {cpu_percent:.2f}% | "
                #     f"RAM: {mem:.2f} MB"
                # )

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue


        active_pids = {p.pid for p in processes}

        for pid in list(previous_cpu_times.keys()):
            if pid not in active_pids:
                del previous_cpu_times[pid]

        # print("------------------------")
        # print(f"Liczba procesów: {len(processes)}")
        # print(
        #     f"SUMA CPU: {total_cpu_percent:.2f}% | "
        #     f"SUMA RAM: {total_mem:.2f} MB"
        # )
        # print("------------------------")

        cpu_samples.append(total_cpu_percent)
        mem_samples.append(total_mem)

        state = "idle" if total_cpu_percent < 20 else "active"

        states.append(state)
        previous_wall_time = current_wall_time
        time.sleep(interval)