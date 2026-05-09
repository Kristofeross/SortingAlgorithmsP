import sqlite3

def get_data_from_db(table_name, set_size, db_path="../dane.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT value FROM {table_name} WHERE set_size = ?", (set_size,))
    rows = cursor.fetchall()
    conn.close()

    return [row[0] for row in rows]