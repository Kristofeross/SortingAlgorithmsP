import multiprocessing as mp

from core.menu import choose_program_mode
from core.auto_benchmark_runner import run_auto_benchmarks
from core.diagnostic_runner import run_process_diagnostics
from core.quick_auto_benchmark_runner import run_quick_auto_benchmarks

from core.results_database import show_algorithm_summary, show_timeout_tests, show_status_summary, show_failed_tests, show_problem_results
from core.results_database import show_process_diagnostics, show_process_diagnostic_events_compact

def main():
    mode = choose_program_mode()

    if mode == "1":
        # run_auto_benchmarks()
        print("Nie to")
    elif mode == "2":
        # run_process_diagnostics()
        print("Nie to")
    elif mode == "3":
        # run_quick_auto_benchmarks()
        print("Nie to")
    elif mode == "4":
        show_algorithm_summary()
        show_timeout_tests()
        show_status_summary()
        show_failed_tests()
        show_problem_results()
    elif mode == "5":
        show_process_diagnostics(),
        show_process_diagnostic_events_compact(14)
    else:
        print("Niepoprawny wybór")

if __name__ == "__main__":
    mp.freeze_support()
    main()