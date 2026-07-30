from core.results_database import (show_system_info, show_results, show_problem_results, show_failed_tests, show_timeout_tests,
    show_status_summary, show_algorithm_summary, clear_results, count_results, show_algorithm_results)


if __name__ == "__main__":
    show_system_info()
    # show_results(limit=50)
    # clear_results()
    show_algorithm_summary()
    show_timeout_tests()
    show_status_summary()
    show_failed_tests()
    show_problem_results()
    count_results()
    # show_algorithm_results("Bucket Sort", limit=50)
