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


def create_process_diagnostics_tables(db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS process_diagnostics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            algorithm TEXT NOT NULL,
            dataset TEXT NOT NULL,
            data_size INTEGER NOT NULL,
            cores INTEGER NOT NULL,

            max_depth INTEGER,
            min_size INTEGER,

            parallel_cutoff INTEGER,
            group_cutoff INTEGER,

            created_processes INTEGER NOT NULL,
            unique_child_processes INTEGER NOT NULL,
            worker_starts INTEGER NOT NULL,

            peak_active_processes INTEGER NOT NULL,

            sequential_fragments INTEGER NOT NULL,

            cutoff_fallbacks INTEGER NOT NULL DEFAULT 0,
            max_depth_fallbacks INTEGER NOT NULL DEFAULT 0,
            cutoff_and_max_depth_fallbacks INTEGER NOT NULL DEFAULT 0,

            parallel_cutoff_fallbacks INTEGER NOT NULL DEFAULT 0,
            group_cutoff_fallbacks INTEGER NOT NULL DEFAULT 0,

            max_reached_depth INTEGER,

            process_start_count INTEGER,
            avg_process_start_time REAL,
            median_process_start_time REAL,
            min_process_start_time REAL,
            max_process_start_time REAL,
            total_process_start_time REAL,

            worker_startup_count INTEGER,
            avg_worker_startup_time REAL,
            median_worker_startup_time REAL,
            min_worker_startup_time REAL,
            max_worker_startup_time REAL,
            total_worker_startup_time REAL,

            correctness TEXT NOT NULL,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS process_diagnostic_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            diagnostic_id INTEGER NOT NULL,

            event TEXT NOT NULL,

            spawn_id TEXT,

            pid INTEGER,
            parent_pid INTEGER,
            child_pid INTEGER,

            depth INTEGER,
            fragment_size INTEGER,

            low INTEGER,
            high INTEGER,

            reason TEXT,
            phase TEXT,

            group_index INTEGER,
            group_size INTEGER,

            bucket_count INTEGER,
            group_count INTEGER,

            max_depth INTEGER,
            min_size INTEGER,

            parallel_cutoff INTEGER,
            group_cutoff INTEGER,

            time_offset REAL,

            FOREIGN KEY (diagnostic_id) REFERENCES process_diagnostics(id) ON DELETE CASCADE
        )
        """
    )


    cursor.execute("PRAGMA table_info(process_diagnostics)")

    diagnostic_columns = {
        row["name"] for row in cursor.fetchall()
    }

    new_diagnostic_columns = {
        "process_start_count": "INTEGER",
        "avg_process_start_time": "REAL",
        "median_process_start_time": "REAL",
        "min_process_start_time": "REAL",
        "max_process_start_time": "REAL",
        "total_process_start_time": "REAL",

        "worker_startup_count": "INTEGER",
        "avg_worker_startup_time": "REAL",
        "median_worker_startup_time": "REAL",
        "min_worker_startup_time": "REAL",
        "max_worker_startup_time": "REAL",
        "total_worker_startup_time": "REAL",
    }

    for column_name, column_type in new_diagnostic_columns.items():
        if column_name not in diagnostic_columns:
            cursor.execute(f""" ALTER TABLE process_diagnostics ADD COLUMN {column_name} {column_type}"""
            )

    cursor.execute("PRAGMA table_info(process_diagnostic_events)")

    event_columns = {
        row["name"] for row in cursor.fetchall()
    }

    if "spawn_id" not in event_columns:
        cursor.execute("ALTER TABLE process_diagnostic_events ADD COLUMN spawn_id TEXT")

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_process_diagnostics_lookup ON process_diagnostics(algorithm, dataset, data_size, cores)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_process_diagnostic_events ON process_diagnostic_events(diagnostic_id)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_process_diagnostic_spawn ON process_diagnostic_events(diagnostic_id, spawn_id)
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


def save_process_diagnostic(algorithm, dataset, data_size, cores, summary, correctness,
    max_depth=None, min_size=None, parallel_cutoff=None, group_cutoff=None, db_path=DB_PATH,):

    conn = get_connection(db_path)
    cursor = conn.cursor()

    sequential_reasons = summary.get("sequential_reasons", {})
    spawn_statistics = summary.get("spawn_statistics", {})
    process_start = spawn_statistics.get("process_start", {})
    worker_startup = spawn_statistics.get("worker_startup", {})

    cursor.execute(
        """
        INSERT INTO process_diagnostics (
            algorithm,
            dataset,
            data_size,
            cores,

            max_depth,
            min_size,

            parallel_cutoff,
            group_cutoff,

            created_processes,
            unique_child_processes,
            worker_starts,

            peak_active_processes,

            sequential_fragments,

            cutoff_fallbacks,
            max_depth_fallbacks,
            cutoff_and_max_depth_fallbacks,

            parallel_cutoff_fallbacks,
            group_cutoff_fallbacks,

            max_reached_depth,

            process_start_count,
            avg_process_start_time,
            median_process_start_time,
            min_process_start_time,
            max_process_start_time,
            total_process_start_time,

            worker_startup_count,
            avg_worker_startup_time,
            median_worker_startup_time,
            min_worker_startup_time,
            max_worker_startup_time,
            total_worker_startup_time,

            correctness
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            algorithm,
            dataset,
            data_size,
            cores,

            max_depth,
            min_size,

            parallel_cutoff,
            group_cutoff,

            summary["created_processes"],
            summary["unique_child_processes"],
            summary["worker_starts"],

            summary["peak_active_processes"],

            summary["sequential_fragments"],

            sequential_reasons.get("cutoff", 0,),
            sequential_reasons.get("max_depth", 0,),
            sequential_reasons.get("cutoff_and_max_depth", 0,),
            sequential_reasons.get("parallel_cutoff", 0,),
            sequential_reasons.get("group_cutoff", 0,),

            summary["max_reached_depth"],

            process_start.get("count", 0,),
            process_start.get("avg"),
            process_start.get("median"),
            process_start.get("min"),
            process_start.get("max"),
            process_start.get("total"),

            worker_startup.get("count", 0,),
            worker_startup.get("avg"),
            worker_startup.get("median"),
            worker_startup.get("min"),
            worker_startup.get("max"),
            worker_startup.get("total"),

            correctness,
        )
    )

    diagnostic_id = cursor.lastrowid

    conn.commit()
    conn.close()

    return diagnostic_id


def save_process_diagnostic_events(diagnostic_id, events, db_path=DB_PATH,):
    if not events:
        return

    conn = get_connection(db_path)
    cursor = conn.cursor()

    timestamps = [
        event["timestamp"]
        for event in events
        if event.get("timestamp") is not None
    ]

    if timestamps:
        root_start_timestamp = min(timestamps)
    else:
        root_start_timestamp = None

    rows = []

    for event in events:
        timestamp = event.get("timestamp")

        time_offset = (
            timestamp - root_start_timestamp
            if timestamp is not None and root_start_timestamp is not None
            else None
        )

        rows.append(
            (
                diagnostic_id,

                event.get("event"),

                event.get("spawn_id"),

                event.get("pid"),
                event.get("parent_pid"),
                event.get("child_pid"),

                event.get("depth"),
                event.get("fragment_size"),

                event.get("low"),
                event.get("high"),

                event.get("reason"),
                event.get("phase"),

                event.get("group_index"),
                event.get("group_size"),

                event.get("bucket_count"),
                event.get("group_count"),

                event.get("max_depth"),
                event.get("min_size"),

                event.get("parallel_cutoff"),
                event.get("group_cutoff"),

                time_offset,
            )
        )

    cursor.executemany(
        """
        INSERT INTO process_diagnostic_events (
            diagnostic_id,

            event,

            spawn_id,

            pid,
            parent_pid,
            child_pid,

            depth,
            fragment_size,

            low,
            high,

            reason,
            phase,

            group_index,
            group_size,

            bucket_count,
            group_count,

            max_depth,
            min_size,

            parallel_cutoff,
            group_cutoff,

            time_offset
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

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


def show_process_diagnostics(limit=20, db_path=DB_PATH,):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            id,
            algorithm,
            dataset,
            data_size,
            cores,

            max_depth,
            min_size,
            parallel_cutoff,
            group_cutoff,

            created_processes,
            unique_child_processes,
            worker_starts,
            peak_active_processes,

            sequential_fragments,

            cutoff_fallbacks,
            max_depth_fallbacks,
            cutoff_and_max_depth_fallbacks,
            parallel_cutoff_fallbacks,
            group_cutoff_fallbacks,

            max_reached_depth,

            process_start_count,
            avg_process_start_time,
            median_process_start_time,
            min_process_start_time,
            max_process_start_time,
            total_process_start_time,

            worker_startup_count,
            avg_worker_startup_time,
            median_worker_startup_time,
            min_worker_startup_time,
            max_worker_startup_time,
            total_worker_startup_time,

            correctness

        FROM process_diagnostics ORDER BY id DESC LIMIT ?
        """,
        (limit,)
    )

    rows = cursor.fetchall()
    conn.close()

    print("\n===== Diagnostyka procesów =====")

    if not rows:
        print("Brak zapisanych wyników diagnostycznych")
        return

    for row in rows:
        def format_time(value):
            if value is None:
                return "-"
            return f"{value:.6f} s"

        print(
            f"""
            ----------------------------------------
            ID diagnostyki:       {row["id"]}
            Algorytm:             {row["algorithm"]}
            Dane:                 {row["dataset"]}
            Rozmiar:              {row["data_size"]}
            Cores:                {row["cores"]}

            max_depth:            {row["max_depth"] if row["max_depth"] is not None else "-"}
            min_size:             {row["min_size"] if row["min_size"] is not None else "-"}
            parallel_cutoff:      {row["parallel_cutoff"] if row["parallel_cutoff"] is not None else "-"}
            group_cutoff:         {row["group_cutoff"] if row["group_cutoff"] is not None else "-"}

            Utworzone procesy potomne: {row["created_processes"]}
            Unikalne procesy potomne:  {row["unique_child_processes"]}
            Uruchomienia workerów:      {row["worker_starts"]}
            Maks. procesów algorytmu:   {row["peak_active_processes"]}

            Sekwencyjne fragmenty:      {row["sequential_fragments"]}

            Cutoff:                     {row["cutoff_fallbacks"]}
            Max depth:                  {row["max_depth_fallbacks"]}
            Cutoff + max depth:         {row["cutoff_and_max_depth_fallbacks"]}
            Parallel cutoff:            {row["parallel_cutoff_fallbacks"]}
            Group cutoff:               {row["group_cutoff_fallbacks"]}

            Maks. głębokość:            {row["max_reached_depth"] if row["max_reached_depth"] is not None else "-"}

            --- Process.start() ---
            Liczba pomiarów:            {row["process_start_count"]}
            Średnia:                    {format_time(row["avg_process_start_time"])}
            Mediana:                    {format_time(row["median_process_start_time"])}
            Minimum:                    {format_time(row["min_process_start_time"])}
            Maksimum:                   {format_time(row["max_process_start_time"])}
            Suma:                       {format_time(row["total_process_start_time"])}

            --- Do WORKER_START ---
            Liczba pomiarów:            {row["worker_startup_count"]}
            Średnia:                    {format_time(row["avg_worker_startup_time"])}
            Mediana:                    {format_time(row["median_worker_startup_time"])}
            Minimum:                    {format_time(row["min_worker_startup_time"])}
            Maksimum:                   {format_time(row["max_worker_startup_time"])}
            Suma:                       {format_time(row["total_worker_startup_time"])}

            Poprawność:                 {row["correctness"]}
            ----------------------------------------
            """
        )


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


def clear_process_diagnostics(db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM process_diagnostic_events")
        cursor.execute("DELETE FROM process_diagnostics")
        cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('process_diagnostic_events', 'process_diagnostics')")

        conn.commit()

        print("Wyczyszczono tabele diagnostyczne: process_diagnostics oraz process_diagnostic_events")

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def show_process_diagnostic_events(diagnostic_id, db_path=DB_PATH,):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            id,
            diagnostic_id,
            event,
            spawn_id,

            pid,
            parent_pid,
            child_pid,

            depth,
            fragment_size,

            low,
            high,

            reason,
            phase,

            group_index,
            group_size,

            bucket_count,
            group_count,

            max_depth,
            min_size,

            parallel_cutoff,
            group_cutoff,

            time_offset

        FROM process_diagnostic_events WHERE diagnostic_id = ? ORDER BY time_offset, id
        """,
        (diagnostic_id,)
    )

    rows = cursor.fetchall()
    conn.close()

    print(f"\n===== Zdarzenia diagnostyki ID={diagnostic_id} =====")

    if not rows:
        print("Brak zapisanych zdarzeń dla tej diagnostyki")
        return

    for row in rows:
        print(
            f"""
            ----------------------------------------
            Event ID:         {row["id"]}
            Zdarzenie:        {row["event"]}
            Spawn ID:         {row["spawn_id"] if row["spawn_id"] else "-"}

            Czas:             {"-" if row["time_offset"] is None else f'{row["time_offset"]:.6f} s'}

            PID:              {row["pid"]}
            Parent PID:       {row["parent_pid"]}
            Child PID:        {row["child_pid"]}

            Głębokość:        {row["depth"]}
            Rozmiar:          {row["fragment_size"]}
            Zakres:           ({row["low"]}, {row["high"]})

            Faza:             {row["phase"]}
            Grupa:            {row["group_index"]}
            Rozmiar grupy:    {row["group_size"]}

            Liczba kubełków:  {row["bucket_count"]}
            Liczba grup:      {row["group_count"]}

            max_depth:        {row["max_depth"]}
            min_size:         {row["min_size"]}
            parallel_cutoff:  {row["parallel_cutoff"]}
            group_cutoff:     {row["group_cutoff"]}

            Powód:            {row["reason"]}
            ----------------------------------------
            """
        )


def show_process_diagnostic_events_compact(diagnostic_id, db_path=DB_PATH):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            event,
            spawn_id,

            pid,
            parent_pid,
            child_pid,

            depth,
            fragment_size,

            phase,
            group_index,
            group_size,

            reason,
            time_offset

        FROM process_diagnostic_events WHERE diagnostic_id = ? ORDER BY time_offset, id
        """,
        (diagnostic_id,)
    )

    rows = cursor.fetchall()
    conn.close()

    print(f"\n===== Zdarzenia diagnostyki ID={diagnostic_id} =====")

    if not rows:
        print("Brak zapisanych zdarzeń.")
        return

    for row in rows:
        time_text = (
            "-"
            if row["time_offset"] is None
            else f'{row["time_offset"]:.6f}'
        )

        spawn_text = (
            row["spawn_id"][:8]
            if row["spawn_id"]
            else "-"
        )

        print(
            f'{time_text:>10} s | '
            f'{row["event"]:<20} | '
            f'spawn={spawn_text:<8} | '
            f'PID={row["pid"]} | '
            f'parent={row["parent_pid"]} | '
            f'child={row["child_pid"]} | '
            f'depth={row["depth"]} | '
            f'size={row["fragment_size"]} | '
            f'phase={row["phase"]} | '
            f'group={row["group_index"]} | '
            f'group_size={row["group_size"]} | '
            f'reason={row["reason"]}'
        )


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
        FROM benchmark_results WHERE algorithm = ? ORDER BY id DESC LIMIT ?
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
        FROM benchmark_results WHERE dataset = ? ORDER BY algorithm, mode, data_size, cores LIMIT ?
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
