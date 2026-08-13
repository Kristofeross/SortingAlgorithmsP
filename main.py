import multiprocessing as mp

from core.menu import choose_program_mode
from core.auto_benchmark_runner import run_auto_benchmarks
from core.quick_auto_benchmark_runner import run_quick_auto_benchmarks

def main():
    mode = choose_program_mode()

    if mode == "1":
        run_auto_benchmarks()
    # elif mode == "2":
    #     run_quick_auto_benchmarks()
    else:
        print("Niepoprawny wybór")

if __name__ == "__main__":
    mp.freeze_support()
    main()