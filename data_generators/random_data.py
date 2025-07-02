import sqlite3
import random

def create_table(conn, table_name):
    try:
        cursor = conn.cursor()
        if table_name == "random_int":
            data_type = "INTEGER"
        else:
            data_type = "REAL"

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                value {data_type} NOT NULL,
                set_size INTEGER NOT NULL CHECK( set_size IN (1000, 10000, 100000) )
            );
        """)
        conn.commit()

    except sqlite3.Error as e:
        print(f"Bład podczas tworzenia tabeli '{table_name}': {e}")

def generate_insert_values(conn, table_name, count):
    try:
        cursor = conn.cursor()

        if table_name == "random_int":
            values = [ (random.randint(0, 1000_000), count) for _ in range(count) ]
        else:
            values = [(random.uniform(0, 1000_000), count) for _ in range(count)]

        cursor.executemany(f"INSERT INTO {table_name} (value, set_size) VALUES (?,?)", values)
        conn.commit()

        print(f"Wstawiono {count} liczb do tabeli {table_name}")

    except sqlite3.Error as e:
        print(f"Błąd podczas wstawiania danych do tabeli '{table_name}': {e}")

def generate_random(table_name, db_path="dane.db"):
    try:
        conn = sqlite3.connect(db_path)
        create_table(conn, table_name)

        allowed_table_name = {"random_int", "random_float"}
        data_size = [1000, 10_000, 100_000]

        if table_name not in allowed_table_name:
            raise ValueError(f"Niepoprawna nazwa tabeli: '{table_name}'. Dozwolone nazwy: {allowed_table_name}")

        for count in data_size:
            generate_insert_values(conn, table_name, count)

        print("Wszystkie dane zostały wygenerwoane i zapisane")

    except sqlite3.Error as e:
        print(f"Błąd połączenia z bazą danych: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    generate_random("random_int")
    generate_random("random_float")