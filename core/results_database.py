import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "dane.db")


def get_connection(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def create_system_info_table():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS system_info
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cpu_name TEXT NOT NULL,
            physical_cores INTEGER NOT NULL,
            logical_cores INTEGER NOT NULL,
            cpu_frequency REAL,
            ram_gb REAL NOT NULL,
            operating_system TEXT NOT NULL,
            architecture TEXT NOT NULL,
            python_version TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    conn.commit()
    conn.close()


def save_system_info(info):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        DELETE FROM system_info
        """
    )

    cursor.execute(
        """
        INSERT INTO system_info
        (
            cpu_name,
            physical_cores,
            logical_cores,
            cpu_frequency,
            ram_gb,
            operating_system,
            architecture,
            python_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            info["cpu_name"],
            info["physical_cores"],
            info["logical_cores"],
            info["cpu_frequency"],
            info["ram_gb"],
            info["operating_system"],
            info["architecture"],
            info["python_version"]
        )
    )

    conn.commit()
    conn.close()


def show_system_info(db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            cpu_name,
            physical_cores,
            logical_cores,
            cpu_frequency,
            ram_gb,
            operating_system,
            architecture,
            python_version,
            created_at
        FROM system_info LIMIT 1
        """
    )

    row = cursor.fetchone()
    conn.close()

    print("\n===== Informacje o sprzęcie =====")

    if row is None:
        print("Brak zapisanych informacji o sprzęcie.")
        return

    print(
        f"""
        Procesor:             {row["cpu_name"]}
        Rdzenie fizyczne:     {row["physical_cores"]}
        Rdzenie logiczne:     {row["logical_cores"]}
        Taktowanie CPU:       {row["cpu_frequency"]:.0f} MHz
        Pamięć RAM:           {row["ram_gb"]:.2f} GB
        System operacyjny:    {row["operating_system"]}
        Architektura:         {row["architecture"]}
        Python:               {row["python_version"]}
        """
    )


def create_results_table(db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS benchmark_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            algorithm TEXT NOT NULL,
            mode TEXT NOT NULL,
            dataset TEXT NOT NULL,
            data_size INTEGER NOT NULL,
            cores INTEGER NOT NULL,
            avg_time REAL,
            median_time REAL,
            std_time REAL,
            avg_cpu REAL,
            avg_mem REAL,
            max_mem REAL,
            speedup REAL,
            efficiency REAL,
            avg_exact_cpu_time REAL,
            min_sample_count INTEGER,
            status TEXT NOT NULL DEFAULT 'OK',
            correctness TEXT NOT NULL DEFAULT 'UNKNOWN',
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_benchmark_lookup
        ON benchmark_results(
            algorithm,
            mode,
            dataset,
            data_size,
            cores
        )
        """
    )

    conn.commit()
    conn.close()


def save_benchmark_result(algorithm, mode, dataset, data_size, cores, stats, db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
            INSERT INTO benchmark_results (
                algorithm,
                mode,
                dataset,
                data_size,
                cores,
                avg_time,
                median_time,
                std_time,
                avg_cpu,
                avg_mem,
                max_mem,
                speedup,
                efficiency,
                avg_exact_cpu_time,
                min_sample_count,
                status,
                correctness,
                error_message
            )VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
            algorithm,
            mode,
            dataset,
            data_size,
            cores,
            stats["avg_time"],
            stats["median_time"],
            stats["std_time"],
            stats["avg_cpu"],
            stats["avg_mem"],
            stats["max_mem"],
            stats["speedup"],
            stats["efficiency"],
            stats["avg_exact_cpu_time"],
            stats["min_sample_count"],
            stats["status"],
            stats["correctness"],
            stats["error_message"],
        ))
    conn.commit()
    conn.close()


def format_result_row(row, include_status=True) -> str:
    text = f"""
            ----------------------------------------
            Algorytm:      {row["algorithm"]}
            Tryb:          {row["mode"]}
            Dane:          {row["dataset"]}
            Rozmiar:       {row["data_size"]}
            Rdzenie:       {row["cores"]}

            Czas średni:       {"-" if row["avg_time"] is None else f'{row["avg_time"]:.6f} s'}
            Mediana:           {"-" if row["median_time"] is None else f'{row["median_time"]:.6f} s'}
            Odchylenie std:    {"-" if row["std_time"] is None else f'{row["std_time"]:.6f} s'}
            CPU (próbkowane):  {"-" if row["avg_cpu"] is None else f'{row["avg_cpu"]:.2f} %'}
            CPU dokładny:      {"-" if row["avg_exact_cpu_time"] is None else f'{row["avg_exact_cpu_time"]:.4f} s'}
            RAM średni:        {"-" if row["avg_mem"] is None else f'{row["avg_mem"]:.2f} MB'}
            RAM max:           {"-" if row["max_mem"] is None else f'{row["max_mem"]:.2f} MB'}
            Min. próbek:       {"-" if row["min_sample_count"] is None else row["min_sample_count"]}

            Speedup:       {"-" if row["speedup"] is None else f'{row["speedup"]:.4f}'}
            Efficiency:    {"-" if row["efficiency"] is None else f'{row["efficiency"]:.4f}'}
    """

    if include_status:
        text += f"""
            Status:        {row["status"]}
            Poprawność:    {row["correctness"]}
            Powód:         {row["error_message"] if row["error_message"] else "-"}
    """

    text += "            ----------------------------------------"
    return text


def show_results(limit=20, db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
            SELECT
                algorithm,
                mode,
                dataset,
                data_size,
                cores,
                avg_time,
                median_time,
                std_time,
                avg_cpu,
                avg_mem,
                max_mem,
                speedup,
                efficiency,
                avg_exact_cpu_time,
                min_sample_count,
                status,
                correctness,
                error_message
            FROM benchmark_results ORDER BY id DESC LIMIT ?
            """, (limit,)
    )

    rows = cursor.fetchall()
    conn.close()

    for row in rows:
        print(format_result_row(row, include_status=True))


def show_failed_tests(db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            id,
            algorithm,
            mode,
            dataset,
            data_size,
            cores,
            status,
            correctness,
            error_message
        FROM benchmark_results
        WHERE status != 'OK' OR correctness != 'CORRECT' ORDER BY id
        """
    )

    rows = cursor.fetchall()
    conn.close()

    print("\n===== Nieudane testy =====")

    if not rows:
        print("Brak problemów. Wszystkie testy poprawne.")
        return

    for row in rows:
        print(
            f"""
                ID: {row["id"]}
                Algorytm: {row["algorithm"]}
                Tryb: {row["mode"]}
                Dane: {row["dataset"]}
                Rozmiar: {row["data_size"]}
                Rdzenie: {row["cores"]}

                Status: {row["status"]}
                Poprawność: {row["correctness"]}
                Błąd: {row["error_message"]}
                --------------------------
            """
        )


def show_timeout_tests(db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            algorithm,
            mode,
            dataset,
            data_size,
            cores,
            error_message
        FROM benchmark_results WHERE status = 'TIMEOUT'
        """
    )

    rows = cursor.fetchall()
    conn.close()

    print("\n===== Timeouty =====")

    if not rows:
        print("Brak timeoutów.")
        return

    for row in rows:
        print(
            f"""
                Algorytm: {row["algorithm"]}
                Tryb: {row["mode"]}
                Dane: {row["dataset"]}
                Rozmiar: {row["data_size"]}
                Rdzenie: {row["cores"]}
                Info: {row["error_message"]}
                ------------------
            """
        )


def show_problem_results(db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            algorithm,
            mode,
            dataset,
            data_size,
            cores,
            status,
            correctness,
            error_message
        FROM benchmark_results
        WHERE status != 'OK' OR correctness != 'CORRECT' ORDER BY algorithm, mode, data_size, cores
        """
    )

    rows = cursor.fetchall()
    conn.close()

    print("\n===== Problematyczne wyniki =====")

    if not rows:
        print("Nie znaleziono problematycznych wyników.")
        return

    for row in rows:
        print(
            f"""
            ----------------------------------------
            Algorytm:      {row["algorithm"]}
            Tryb:          {row["mode"]}
            Dane:          {row["dataset"]}
            Rozmiar:       {row["data_size"]}
            Rdzenie:       {row["cores"]}
            Status:        {row["status"]}
            Poprawność:    {row["correctness"]}
            Powód:         {row["error_message"]}
            ----------------------------------------
            """
        )


def show_status_summary(db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT status, correctness, COUNT(*) as count FROM benchmark_results GROUP BY status, correctness ORDER BY status, correctness
        """
    )

    rows = cursor.fetchall()
    conn.close()

    print("\n===== Podsumowania statusów =====")

    for row in rows:
        print(
            f"Status: {row['status']:<8}"
            f" Poprawność: {row['correctness']:<10}"
            f" Liczba: {row['count']}"
        )


def show_algorithm_summary(db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT algorithm, mode, COUNT(*) as count FROM benchmark_results GROUP BY algorithm, mode ORDER BY algorithm
        """
    )

    rows = cursor.fetchall()
    conn.close()

    print("\n===== Liczba testów =====")

    for row in rows:
        print(f"{row['algorithm']} | {row['mode']} : {row['count']}")


def clear_results(db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM benchmark_results")

    conn.commit()
    conn.close()

    print("\n=== Baza wyników została wyczyszczona ===")


def count_results(db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) as count FROM benchmark_results")

    count = cursor.fetchone()["count"]
    conn.close()

    print("\n===== Liczba wszystkich testów =====")
    print(f"Liczba zapisanych wyników: {count}")

    return count


def show_algorithm_results(algorithm_name, limit=20, db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            algorithm,
            mode,
            dataset,
            data_size,
            cores,
            avg_time,
            median_time,
            std_time,
            avg_cpu,
            avg_mem,
            max_mem,
            speedup,
            efficiency,
            avg_exact_cpu_time,
            min_sample_count,
            status,
            correctness,
            error_message
        FROM benchmark_results
        WHERE algorithm = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (algorithm_name, limit)
    )

    rows = cursor.fetchall()
    conn.close()

    print(f"\n===== Wyniki: {algorithm_name} =====")

    if not rows:
        print("Brak wyników.")
        return

    for row in rows:
        print(format_result_row(row, include_status=True))


def show_dataset_results(dataset_name, limit=200, db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            algorithm,
            mode,
            dataset,
            data_size,
            cores,
            avg_time,
            median_time,
            std_time,
            avg_cpu,
            avg_mem,
            max_mem,
            speedup,
            efficiency,
            avg_exact_cpu_time,
            min_sample_count
        FROM benchmark_results
        WHERE dataset = ?
        ORDER BY algorithm, mode, data_size, cores
        LIMIT ?
        """,
        (dataset_name, limit)
    )

    rows = cursor.fetchall()
    conn.close()

    print(f"\n===== Wyniki dla zbioru danych: {dataset_name} =====")

    if not rows:
        print("Brak wyników dla tego zbioru danych.")
        return

    for row in rows:
        print(format_result_row(row, include_status=False))
