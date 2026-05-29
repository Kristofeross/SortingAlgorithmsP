from .utils import execute_query


TABLE_NAME = "benchmark_results"


def get_all_results():
    query = f"""SELECT * FROM {TABLE_NAME}"""

    return execute_query(query)


def get_algorithms():
    query = f"""SELECT DISTINCT algorithm FROM {TABLE_NAME} ORDER BY algorithm"""

    return execute_query(query)


def get_datasets():
    query = f"""SELECT DISTINCT dataset FROM {TABLE_NAME} ORDER BY dataset"""

    return execute_query(query)


def get_data_sizes():
    query = f"""SELECT DISTINCT data_size FROM {TABLE_NAME} ORDER BY data_size"""

    return execute_query(query)


def get_core_counts():
    query = f"""SELECT DISTINCT cores FROM {TABLE_NAME} ORDER BY cores"""

    return execute_query(query)


def get_execution_times(algorithm, dataset, data_size):
    query = f"""
        SELECT cores, avg_time
        FROM {TABLE_NAME}
        WHERE algorithm = ? AND dataset = ? AND data_size = ? AND mode = 'Parallel'
        ORDER BY cores
    """

    return execute_query( query, (algorithm, dataset, data_size) )


def get_speedup( algorithm, dataset, data_size):
    query = f"""
        SELECT cores, speedup
        FROM {TABLE_NAME}
        WHERE algorithm = ? AND dataset = ? AND data_size = ? AND mode = 'Parallel'
        ORDER BY cores
    """

    return execute_query( query, (algorithm, dataset, data_size) )


def get_efficiency( algorithm, dataset, data_size ):
    query = f"""
        SELECT cores, efficiency
        FROM {TABLE_NAME}
        WHERE algorithm = ? AND dataset = ? AND data_size = ? AND mode = 'Parallel'
        ORDER BY cores
    """

    return execute_query( query, (algorithm, dataset, data_size) )


def get_cpu_usage( algorithm, dataset, data_size ):
    query = f"""
        SELECT cores, avg_cpu
        FROM {TABLE_NAME}
        WHERE algorithm = ? AND dataset = ? AND data_size = ?
        ORDER BY cores
    """

    return execute_query( query, (algorithm, dataset, data_size) )


def get_memory_usage( algorithm, dataset, data_size ):
    query = f"""
        SELECT cores, avg_mem
        FROM {TABLE_NAME}
        WHERE algorithm = ? AND dataset = ? AND data_size = ?
        ORDER BY cores
    """

    return execute_query( query, (algorithm, dataset, data_size) )


def get_stability( algorithm, dataset, data_size ):
    query = f"""
        SELECT cores,  std_time
        FROM {TABLE_NAME}
        WHERE algorithm = ? AND dataset = ? AND data_size = ?
        ORDER BY cores
    """

    return execute_query( query, (algorithm, dataset, data_size) )