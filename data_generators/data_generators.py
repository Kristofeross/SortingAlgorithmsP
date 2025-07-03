import sqlite3
import random

def create_table(conn, table_name):
    try:
        cursor = conn.cursor()
        if "int" in table_name:
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
        print(f"Błąd podczas tworzenia tabeli '{table_name}': {e}")

def generate_random_values(table_name, count, scope):
    if "int" in table_name:
        return [(random.randint(0, scope), count) for _ in range(count)]
    else:
        return [(random.uniform(0, scope), count) for _ in range(count)]

def generate_duplicate_values(table_name, count, scope):
    unique_count = int(count * 0.6)
    if "int" in table_name:
        unique_values = [random.randint(0, scope) for _ in range(unique_count)]
    else:
        unique_values = [random.uniform(0, scope) for _ in range(unique_count)]

    return [(random.choice(unique_values), count) for _ in range(count)]

def generate_part_sorted_values(table_name, count, scope):
    sorted_count = int(count * 0.4)
    random_count = count - sorted_count
    parts = 5

    if "int" in table_name:
        sorted_values = sorted(random.randint(0, scope) for _ in range(sorted_count))
        random_values = [random.randint(0, scope) for _ in range(random_count)]
    else:
        sorted_values = sorted(random.uniform(0, scope) for _ in range(sorted_count))
        random_values = [random.uniform(0, scope) for _ in range(random_count)]

    # Divine sorted values at parts
    chunk_size = sorted_count // parts
    sorted_chunks = [sorted_values[i*chunk_size:(i+1)*chunk_size] for i in range(parts-1)]
    sorted_chunks.append(sorted_values[(parts-1)*chunk_size:])
    # Divine random values at parts + 1
    random_chunk_size = random_count // (parts + 1)
    random_chunks = [random_values[i*random_chunk_size:(i+1)*random_chunk_size] for i in range(parts)]
    random_chunks.append(random_values[parts*random_chunk_size:])
    # Combine sorted and random parts
    combined = []
    for i in range(parts):
        combined.extend(random_chunks[i])
        combined.extend(sorted_chunks[i])
    combined.extend(random_chunks[-1])

    values = [(val, count) for val in combined]

    return values

def insert_values(conn, table_name, count):
    try:
        cursor = conn.cursor()
        values = []
        scope = 1000_000

        if table_name.startswith("random_"):
            values = generate_random_values(table_name, count, scope)
        elif table_name.startswith("duplicates_"):
            values = generate_duplicate_values(table_name, count, scope)
        elif table_name.startswith("part_sorted_"):
            values = generate_part_sorted_values(table_name, count, scope)
        else:
            raise ValueError(f"Nieobsługiwana tabela: {table_name}")

        cursor.executemany(f"INSERT INTO {table_name} (value, set_size) VALUES (?,?)", values)
        conn.commit()

        print(f"Wstawiono {count} liczb do tabeli {table_name}")

    except sqlite3.Error as e:
        print(f"Błąd podczas wstawiania danych do tabeli '{table_name}': {e}")

def generate_data(table_name, db_path="dane.db"):
    try:
        allowed_table_name = {"random_int", "random_float", "duplicates_int", "duplicates_float", "part_sorted_int", "part_sorted_float"}
        if table_name not in allowed_table_name:
            raise ValueError(f"Niepoprawna nazwa tabeli: '{table_name}'. Dozwolone nazwy: {allowed_table_name}")

        conn = sqlite3.connect(db_path)
        create_table(conn, table_name)

        data_size = [1000, 10_000, 100_000]
        for count in data_size:
            insert_values(conn, table_name, count)

        print("Wszystkie dane zostały wygenerwoane i zapisane")

    except sqlite3.Error as e:
        print(f"Błąd połączenia z bazą danych: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    generate_data("random_int")
    generate_data("random_float")
    generate_data("duplicates_int")
    generate_data("duplicates_float")
    generate_data("part_sorted_int")
    generate_data("part_sorted_float")
