import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "dane.db")

def get_data_from_db(table_name, set_size, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT value FROM {table_name} WHERE set_size = ?", (set_size,))
    rows = cursor.fetchall()
    conn.close()

    return [row[0] for row in rows]