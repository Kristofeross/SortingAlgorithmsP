import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "dane.db"


def get_connection():
    return sqlite3.connect(DB_PATH)


def execute_query(query, params=None):
    conn = get_connection()

    try:
        cursor = conn.cursor()

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        return cursor.fetchall()

    finally:
        conn.close()


def execute_query_with_columns(query, params=None):
    conn = get_connection()

    try:
        cursor = conn.cursor()

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]

        return columns, rows

    finally:
        conn.close()