import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "dane.db")


def create_system_info_table():
    conn = sqlite3.connect(DB_PATH)
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
    conn = sqlite3.connect(DB_PATH)
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
    conn = sqlite3.connect(db_path)
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
        FROM system_info
        LIMIT 1
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
        Procesor:             {row[0]}
        Rdzenie fizyczne:     {row[1]}
        Rdzenie logiczne:     {row[2]}
        Taktowanie CPU:       {row[3]:.0f} MHz
        Pamięć RAM:           {row[4]:.2f} GB
        System operacyjny:    {row[5]}
        Architektura:         {row[6]}
        Python:               {row[7]}
        """
    )


def create_results_table(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
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


def save_benchmark_result( algorithm, mode, dataset, data_size, cores, stats, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
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
            status,
            correctness,
            error_message
        )VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,(
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
        stats["status"],
        stats["correctness"],
        stats["error_message"],
    ))
    conn.commit()
    conn.close()


def show_results(limit=20, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
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
            avg_time,
            median_time,
            std_time,
            avg_cpu,
            avg_mem,
            max_mem,
            speedup,
            efficiency,
            status,
            error_message,
            created_at
        FROM benchmark_results
        ORDER BY id DESC
        LIMIT ?
        """, (limit,)
    )

    rows = cursor.fetchall()
    conn.close()

    for row in rows:
        avg_time = (f"{row[6]:.6f} s" if row[6] is not None else "-")
        median_time = (f"{row[7]:.6f} s" if row[7] is not None else "-")
        std_time = (f"{row[8]:.6f} s" if row[8] is not None else "-")
        cpu = (f"{row[9]:.2f}%" if row[9] is not None else "-")
        avg_ram = (f"{row[10]:.2f} MB" if row[10] is not None else "-")
        max_ram = f"{row[11]:.2f} MB" if row[11] is not None else "-"
        speedup = (f"{row[12]:.4f}" if row[12] is not None else "-")
        efficiency = (f"{row[13]:.4f}" if row[13] is not None else "-")
        error = (row[15] if row[15] is not None else "-")

        print(
            f"""
                Algorytm: {row[1]}
                Tryb: {row[2]}
                Status: {row[14]}
                Zestaw danych: {row[3]}
                Rozmiar danych: {row[4]}
                Rdzenie: {row[5]}
                Średni czas: {avg_time}
                Mediana: {median_time}
                Odchylenie std: {std_time}
                CPU: {cpu}
                Średni RAM: {avg_ram}
                Maksymalny RAM: {max_ram}
                Speedup: {speedup}
                Efficiency: {efficiency}
                Błąd: {error}
            """
        )

# Test for datas
def show_failed_tests(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
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
        WHERE status != 'OK'
           OR correctness != 'CORRECT'
        ORDER BY id
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
                ID: {row[0]}
                Algorytm: {row[1]}
                Tryb: {row[2]}
                Dane: {row[3]}
                Rozmiar: {row[4]}
                Rdzenie: {row[5]}
                
                Status: {row[6]}
                Poprawność: {row[7]}
                Błąd: {row[8]}
                --------------------------
            """
        )


def show_timeout_tests(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
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
        FROM benchmark_results
        WHERE status = 'TIMEOUT'
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
                Algorytm: {row[0]}
                Tryb: {row[1]}
                Dane: {row[2]}
                Rozmiar: {row[3]}
                Rdzenie: {row[4]}
                Info: {row[5]}
                ------------------
            """
        )


def show_problem_results(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
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
        WHERE status != 'OK'
           OR correctness != 'CORRECT'
        ORDER BY algorithm, mode, data_size, cores
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
            Algorytm:      {row[0]}
            Tryb:          {row[1]}
            Dane:          {row[2]}
            Rozmiar:       {row[3]}
            Rdzenie:       {row[4]}
            Status:        {row[5]}
            Poprawność:    {row[6]}
            Powód:         {row[7]}
            ----------------------------------------
            """
        )


def show_status_summary(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            status,
            correctness,
            COUNT(*)
        FROM benchmark_results
        GROUP BY status, correctness
        ORDER BY status, correctness
        """
    )

    rows = cursor.fetchall()
    conn.close()


    print("\n===== Podsumowania statusów =====")

    for status, correctness, count in rows:
        print(
            f"Status: {status:<8}"
            f" Poprawność: {correctness:<10}"
            f" Liczba: {count}"
        )


def show_algorithm_summary(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            algorithm,
            mode,
            COUNT(*)
        FROM benchmark_results
        GROUP BY algorithm, mode
        ORDER BY algorithm
        """
    )

    rows = cursor.fetchall()
    conn.close()

    print("\n===== Liczba testów =====")

    for row in rows:
        print(f"{row[0]} | {row[1]} : {row[2]}")


def clear_results(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        DELETE FROM benchmark_results
        """
    )

    conn.commit()
    conn.close()

    print("\n=== Baza wyników została wyczyszczona ===")


def count_results(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM benchmark_results
        """
    )

    count = cursor.fetchone()[0]
    conn.close()

    print("\n===== Liczba wszystkich testów =====")
    print(f"Liczba zapisanych wyników: {count}")

    return count


def show_algorithm_results(algorithm_name, limit=20, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
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
            avg_cpu,
            avg_mem,
            max_mem,
            speedup,
            efficiency,
            status,
            correctness,
            error_message
        FROM benchmark_results
        WHERE algorithm = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (
            algorithm_name,
            limit
        )
    )

    rows = cursor.fetchall()
    conn.close()

    print(f"\n===== Wyniki: {algorithm_name} =====")

    if not rows:
        print("Brak wyników.")
        return

    for row in rows:
        print(
            f"""
            ----------------------------------------
            Algorytm:      {row[0]}
            Tryb:          {row[1]}
            Dane:          {row[2]}
            Rozmiar:       {row[3]}
            Rdzenie:       {row[4]}
        
            Czas:          {"-" if row[5] is None else f"{row[5]:.6f} s"}
            CPU:           {"-" if row[6] is None else f"{row[6]:.2f} %"}
            RAM średni:    {"-" if row[7] is None else f"{row[7]:.2f} MB"}
            RAM max:       {"-" if row[8] is None else f"{row[8]:.2f} MB"}
        
            Speedup:       {"-" if row[9] is None else f"{row[9]:.4f}"}
            Efficiency:    {"-" if row[10] is None else f"{row[10]:.4f}"}
        
            Status:        {row[11]}
            Poprawność:    {row[12]}
            Powód:         {row[13] if row[13] else "-"}
            ----------------------------------------
            """
        )