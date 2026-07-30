import platform
import psutil
import cpuinfo


def get_system_info():
    cpu = cpuinfo.get_cpu_info()

    return {
        "cpu_name": cpu["brand_raw"],
        "physical_cores": psutil.cpu_count(logical=False),
        "logical_cores": psutil.cpu_count(logical=True),
        "cpu_frequency": psutil.cpu_freq().max,
        "ram_gb": round(
            psutil.virtual_memory().total / (1024 ** 3),
            2
        ),
        "operating_system": (
            f"{platform.system()} "
            f"{platform.release()}"
        ),
        "architecture": platform.machine(),
        "python_version": platform.python_version()
    }


def get_available_cores(use_logical=False):
    physical = psutil.cpu_count(logical=False)
    logical = psutil.cpu_count(logical=True)

    max_cores = logical if use_logical else physical

    available_cores = []
    current = 2

    while current <= max_cores:
        available_cores.append(current)
        current *= 2

    return available_cores, physical, logical