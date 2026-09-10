import math
import multiprocessing as mp
import statistics
from collections import Counter
from queue import Empty

from core.database import get_data_from_db
from core.menu import print_separator
from core.results_database import create_process_diagnostics_tables, save_process_diagnostic, save_process_diagnostic_events

from algorithms.shared_memory.quicksort.parallel import parallel_quicksort
from algorithms.shared_memory.mergesort.parallel import parallel_merge_sort
from algorithms.shared_memory.bucketsort.parallel import parallel_bucket_sort
from algorithms.shared_memory.samplesort.parallel import parallel_sample_sort


DIAGNOSTIC_SCENARIOS = [
    {
        "dataset": "random_int",
        "data_size": 1_000_000,
        "cores": [2, 4, 8, 16, 32],
        "algorithms": ["Quick Sort", "Merge Sort", "Bucket Sort", "Sample Sort"],
    },
]


DIAGNOSTIC_ALGORITHMS = {
    "Quick Sort": {
        "function": parallel_quicksort,
        "parameter": "max_depth",
    },
    "Merge Sort": {
        "function": parallel_merge_sort,
        "parameter": "max_depth",
    },
    "Bucket Sort": {
        "function": parallel_bucket_sort,
        "parameter": "process_count",
    },
    "Sample Sort": {
        "function": parallel_sample_sort,
        "parameter": "process_count",
    },
}


def collect_events(event_queue):
    events = []

    while True:
        try:
            event = event_queue.get(timeout=0.2)
            events.append(event)
        except Empty:
            break

    return events


def calculate_peak_active_processes(events):
    timeline = []

    for event in events:
        event_type = event.get("event")
        timestamp = event.get("timestamp")

        if timestamp is None:
            continue

        if event_type in ("ROOT_START", "WORKER_START",):
            timeline.append( (timestamp, 0, 1, ) )

        elif event_type in ("WORKER_END", "ROOT_END",):
            timeline.append( (timestamp, 1, -1,) )

    timeline.sort(
        key=lambda item: (
            item[0],
            item[1],
        )
    )

    current_processes = 0
    peak_processes = 0

    for _, _, delta in timeline:
        current_processes += delta
        peak_processes = max(peak_processes, current_processes,)

    return peak_processes


def build_time_statistics(values):
    if not values:
        return {
            "count": 0,
            "avg": None,
            "median": None,
            "min": None,
            "max": None,
            "total": None,
        }

    return {
        "count": len(values),
        "avg": statistics.mean(values),
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "total": sum(values),
    }


def calculate_spawn_statistics(events):
    process_events = {}

    for event in events:
        spawn_id = event.get("spawn_id")

        if spawn_id is None:
            continue

        if spawn_id not in process_events:
            process_events[spawn_id] = {}

        process_events[spawn_id][event["event"]] = event

    process_start_times = []
    worker_startup_times = []
    by_phase = {}

    for related_events in process_events.values():
        begin_event = related_events.get("PROCESS_START_BEGIN")

        created_event = related_events.get("PROCESS_CREATED")

        worker_start_event = related_events.get("WORKER_START")

        if begin_event is None or created_event is None:
            continue

        begin_timestamp = begin_event.get("timestamp")
        created_timestamp = created_event.get("timestamp")

        if begin_timestamp is None or created_timestamp is None:
            continue

        process_start_time = (created_timestamp - begin_timestamp)

        process_start_times.append(process_start_time)

        phase = created_event.get("phase") or begin_event.get("phase") or "default"

        if phase not in by_phase:
            by_phase[phase] = {
                "process_start_times": [],
                "worker_startup_times": [],
            }

        by_phase[phase]["process_start_times"].append(process_start_time)

        if worker_start_event is not None:
            worker_start_timestamp = worker_start_event.get("timestamp")

            if worker_start_timestamp is not None:
                worker_startup_time = worker_start_timestamp - begin_timestamp

                worker_startup_times.append(worker_startup_time)

                by_phase[phase]["worker_startup_times"].append(worker_startup_time)

    phase_statistics = {}

    for phase, values in by_phase.items():
        phase_statistics[phase] = {
            "process_start": build_time_statistics(values["process_start_times"]),
            "worker_startup": build_time_statistics(values["worker_startup_times"]),
        }

    return {
        "process_start": build_time_statistics(process_start_times),
        "worker_startup": build_time_statistics(worker_startup_times),
        "by_phase": phase_statistics,
    }


def analyze_events(events):
    created_processes = [
        event
        for event in events
        if event["event"] == "PROCESS_CREATED"
    ]

    worker_starts = [
        event
        for event in events
        if event["event"] == "WORKER_START"
    ]

    sequential_events = [
        event
        for event in events
        if event["event"] == "SEQUENTIAL"
    ]

    process_pids = {
        event["child_pid"]
        for event in created_processes
        if event.get("child_pid") is not None
    }

    max_reached_depth = max(
        (
            event["depth"]
            for event in events
            if event.get("depth") is not None
        ),
        default=None,
    )

    sequential_reasons = Counter(
        event.get("reason")
        for event in sequential_events
        if event.get("reason") is not None
    )

    created_by_phase = Counter(
        event.get("phase")
        for event in created_processes
        if event.get("phase") is not None
    )

    workers_by_phase = Counter(
        event.get("phase")
        for event in worker_starts
        if event.get("phase") is not None
    )

    sequential_by_phase = Counter(
        event.get("phase")
        for event in sequential_events
        if event.get("phase") is not None
    )

    peak_active_processes = calculate_peak_active_processes(events)
    spawn_statistics = calculate_spawn_statistics(events)

    return {
        "created_processes": len(created_processes),
        "unique_child_processes": len(process_pids),
        "worker_starts": len(worker_starts),
        "sequential_fragments": len(sequential_events),
        "sequential_reasons": sequential_reasons,
        "created_by_phase": created_by_phase,
        "workers_by_phase": workers_by_phase,
        "sequential_by_phase": sequential_by_phase,
        "max_reached_depth": max_reached_depth,
        "peak_active_processes": peak_active_processes,
        "spawn_statistics": spawn_statistics,
    }


def print_event(event):
    spawn_id = event.get("spawn_id")

    spawn_text = spawn_id[:8] if spawn_id else "-"

    print(
        f"{event['event']:<20} | "
        f"PID={event.get('pid')} | "
        f"parent={event.get('parent_pid')} | "
        f"child={event.get('child_pid')} | "
        f"spawn={spawn_text} | "
        f"depth={event.get('depth')} | "
        f"size={event.get('fragment_size')} | "
        f"range=({event.get('low')}, {event.get('high')}) | "
        f"phase={event.get('phase')} | "
        f"group={event.get('group_index')} | "
        f"group_size={event.get('group_size')} | "
        f"reason={event.get('reason')}"
    )


def print_time_statistics(label, statistics_data,):
    if statistics_data["count"] == 0:
        print(f"{label}: brak pomiarów")
        return

    print(f"{label}:")
    print(f"  liczba pomiarów: {statistics_data['count']}")
    print(f"  średnia: {statistics_data['avg']:.6f} s")
    print(f"  mediana: {statistics_data['median']:.6f} s")
    print(f"  minimum: {statistics_data['min']:.6f} s")
    print(f"  maksimum: {statistics_data['max']:.6f} s")
    print(f"  suma: {statistics_data['total']:.6f} s")


def print_spawn_statistics(spawn_statistics,):
    print("\nCzasy uruchamiania procesów:")

    print_time_statistics(
        "Process.start()",
        spawn_statistics["process_start"],
    )

    print()

    print_time_statistics("PROCESS_START_BEGIN -> WORKER_START", spawn_statistics["worker_startup"],)
    phase_statistics = spawn_statistics.get("by_phase", {})

    if not phase_statistics:
        return

    meaningful_phases = {
        phase: stats
        for phase, stats
        in phase_statistics.items()
        if phase != "default"
    }

    if not meaningful_phases:
        return

    print("\nCzasy według faz:")

    for (phase, phase_stats,) in meaningful_phases.items():
        print(f"\nFaza: {phase}")

        print_time_statistics("  Process.start()", phase_stats["process_start"],)
        print_time_statistics("  Do WORKER_START", phase_stats["worker_startup"],)


def print_summary( algorithm_name, cores, max_depth, events, result, expected, dataset, data_size,):
    summary = analyze_events(events)

    root_start = next(
        (
            event
            for event in events
            if event["event"] == "ROOT_START"
        ),
        None,
    )

    min_size = (
        root_start.get("min_size")
        if root_start is not None
        else None
    )

    parallel_cutoff = (
        root_start.get("parallel_cutoff")
        if root_start is not None
        else None
    )

    group_cutoff = (
        root_start.get("group_cutoff")
        if root_start is not None
        else None
    )

    print("\nPodsumowanie diagnostyczne:\n")

    print(f"Algorytm: {algorithm_name}")
    print(f"Zestaw danych: {dataset}")
    print(f"Rozmiar danych: {data_size}")
    print(f"Konfiguracja cores: {cores}")

    if max_depth is not None:
        print(f"max_depth: {max_depth}")

    if min_size is not None:
        print(f"min_size: {min_size}")

    if parallel_cutoff is not None:
        print(f"parallel_cutoff: {parallel_cutoff}")

    if group_cutoff is not None:
        print(f"group_cutoff: {group_cutoff}")

    print(f"Utworzone procesy potomne: {summary['created_processes']}")
    print(f"Unikalne procesy potomne: {summary['unique_child_processes']}")
    print(f"Uruchomienia workerów: {summary['worker_starts']}")
    print(f"Maksymalna liczba jednocześnie uruchomionych procesów algorytmu: {summary['peak_active_processes']}")
    print(f"Fragmenty/grupy wykonane sekwencyjnie: {summary['sequential_fragments']}")

    if summary["max_reached_depth"] is not None:
        print(f"Maksymalna osiągnięta głębokość: {summary['max_reached_depth']}")

    if summary["sequential_reasons"]:
        print("Powody wykonania sekwencyjnego:")

        for (reason, count,) in summary["sequential_reasons"].items():
            print(f"  {reason}: {count}")

    if summary["created_by_phase"]:
        print("Utworzone procesy według faz:")

        for (phase,count,) in summary["created_by_phase"].items():
            print(f"  {phase}: {count}")

    if summary["workers_by_phase"]:
        print("Uruchomienia workerów według faz:")

        for (phase, count,) in summary["workers_by_phase"].items():
            print(f"  {phase}: {count}")

    if summary["sequential_by_phase"]:
        print("Wykonania sekwencyjne według faz:")

        for (phase, count,) in summary["sequential_by_phase"].items():
            print(f"  {phase}: {count}")

    print_spawn_statistics(summary["spawn_statistics"])

    correctness = "CORRECT" if result == expected else "INCORRECT"

    print(f"\nPoprawność sortowania: {correctness}")

    return {
        "summary": summary,
        "correctness": correctness,
        "min_size": min_size,
        "parallel_cutoff": parallel_cutoff,
        "group_cutoff": group_cutoff,
    }


def validate_scenario(scenario):
    required_fields = {
        "dataset",
        "data_size",
        "cores",
        "algorithms",
    }

    missing_fields = required_fields - set(scenario.keys())

    if missing_fields:
        raise ValueError("Brak wymaganych pól scenariusza: {sorted(missing_fields)}")

    if not scenario["dataset"]:
        raise ValueError("Nazwa datasetu nie może być pusta.")

    if not isinstance(scenario["data_size"], int,) or scenario["data_size"] <= 0:
        raise ValueError("data_size musi być dodatnią liczbą całkowitą.")

    if not scenario["cores"]:
        raise ValueError("Lista cores nie może być pusta.")

    if not scenario["algorithms"]:
        raise ValueError("Lista algorithms nie może być pusta.")

    for cores in scenario["cores"]:
        if not isinstance(cores, int,) or cores <= 0:
            raise ValueError(f"Nieprawidłowa wartość cores: {cores}")

    for algorithm_name in scenario["algorithms"]:
        if algorithm_name not in DIAGNOSTIC_ALGORITHMS:
            raise ValueError(f"Nieznany algorytm diagnostyczny: {algorithm_name}")

        parameter_type = DIAGNOSTIC_ALGORITHMS[algorithm_name]["parameter"]

        if parameter_type == "max_depth":
            for cores in scenario["cores"]:
                if ( cores & (cores - 1) ) != 0:
                    raise ValueError(f"{algorithm_name}: cores={cores} nie jest potęgą liczby 2. Dla algorytmów opartych na max_depth użyj np. 1, 2, 4, 8, 16, 32.")


def run_single_diagnostic(algorithm_name, algorithm_config, data, cores, dataset, data_size,):
    algorithm_function = algorithm_config["function"]
    parameter_type = algorithm_config["parameter"]

    max_depth = None

    if parameter_type == "max_depth":
        max_depth = int(math.log2(cores))

    print("\nDiagnostyka procesów")
    print_separator()

    print(f"Algorytm: {algorithm_name}")
    print(f"Dane: {dataset}")
    print(f"Rozmiar: {len(data)}")
    print(f"cores: {cores}")

    expected = sorted(data)

    with mp.Manager() as manager:
        event_queue = manager.Queue()

        if parameter_type == "max_depth":
            result = algorithm_function(
                list(data),
                max_depth,
                event_queue=event_queue,
            )
        elif parameter_type == "process_count":
            result = algorithm_function(
                list(data),
                cores,
                event_queue=event_queue,
            )
        else:
            raise ValueError(f"Nieznany typ parametru diagnostycznego: {parameter_type}")

        events = collect_events(event_queue)

    events.sort(key=lambda event: (event["timestamp"]))

    print("\n--- Zdarzenia ---")

    for event in events:
        print_event(event)

    diagnostic_result = print_summary(
        algorithm_name=algorithm_name,
        cores=cores,
        max_depth=max_depth,
        events=events,
        result=result,
        expected=expected,
        dataset=dataset,
        data_size=data_size,
    )

    diagnostic_id = save_process_diagnostic(
        algorithm=algorithm_name,
        dataset=dataset,
        data_size=data_size,
        cores=cores,
        summary=diagnostic_result["summary"],
        correctness=diagnostic_result["correctness"],
        max_depth=max_depth,
        min_size=diagnostic_result["min_size"],
        parallel_cutoff=diagnostic_result["parallel_cutoff"],
        group_cutoff=diagnostic_result["group_cutoff"],
    )

    save_process_diagnostic_events(diagnostic_id=diagnostic_id, events=events,)

    return events


def run_process_diagnostics():
    create_process_diagnostics_tables()

    print("Test diagnostyczny procesów")

    for scenario in DIAGNOSTIC_SCENARIOS:
        validate_scenario(scenario)

        dataset = scenario["dataset"]
        data_size = scenario["data_size"]
        cores_list = scenario["cores"]
        algorithm_names = scenario["algorithms"]

        print(
            f"Scenariusz: "
            f"{dataset}, "
            f"{data_size}, "
            f"cores={cores_list}"
        )

        data = get_data_from_db(dataset, data_size,)

        if len(data) != data_size:
            raise ValueError(f"Nieprawidłowa liczba danych dla {dataset}: oczekiwano {data_size}, pobrano {len(data)}")

        print(f"Pobrano {len(data)} rekordów")

        for algorithm_name in algorithm_names:
            algorithm_config = DIAGNOSTIC_ALGORITHMS[algorithm_name]

            for cores in cores_list:
                run_single_diagnostic(
                    algorithm_name=algorithm_name,
                    algorithm_config=algorithm_config,
                    data=data,
                    cores=cores,
                    dataset=dataset,
                    data_size=data_size,
                )

    print("Diagnostyka zakończona")


if __name__ == "__main__":
    mp.freeze_support()
    run_process_diagnostics()
