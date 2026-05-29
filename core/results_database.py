import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "dane.db")


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
            avg_time REAL NOT NULL,
            median_time REAL NOT NULL,
            std_time REAL NOT NULL,
            avg_cpu REAL NOT NULL,
            avg_mem REAL NOT NULL,
            speedup REAL,
            efficiency REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            speedup,
            efficiency
        )VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        stats["speedup"],
        stats["efficiency"]
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
            speedup,
            efficiency,
            created_at
        FROM benchmark_results
        ORDER BY id DESC
        LIMIT ?
        """, (limit,))

    rows = cursor.fetchall()
    conn.close()

    for row in rows:
        print(
            f"""
                Algorytm: {row[1]}
                Tryb: {row[2]}
                Zestaw danych: {row[3]}
                Rozmiar danych: {row[4]}
                Rdzenie: {row[5]}
                Średni czas: {row[6]:.6f} s
                Mediana: {row[7]:.6f} s
                Odchylenie std: {row[8]:.6f} s
                CPU: {row[9]:.2f}%
                RAM: {row[10]:.2f} MB
                Speedup: {row[11]}
                Efficiency: {row[12]}
            """
        )
