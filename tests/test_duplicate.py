import unittest
import sqlite3
import os

from data_generators.data_generators import (
    create_table,
    insert_values,
    generate_data,
    generate_duplicate_values
)

class MyTestCase(unittest.TestCase):
    test_db = "test_dane.db"

    def setUp(self):
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
        self.conn = sqlite3.connect(self.test_db)

    def tearDown(self):
        self.conn.close()
        if os.path.exists(self.test_db):
            os.remove(self.test_db)# add assertion here

    def test_create_table_int(self):
        create_table(self.conn, "duplicates_int")
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(duplicates_int)")
        columns = [col[1] for col in cursor.fetchall()]
        self.assertIn("id", columns)
        self.assertIn("value", columns)
        self.assertIn("set_size", columns)

    def test_create_table_float(self):
        create_table(self.conn, "duplicates_float")
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(duplicates_float)")
        columns = [col[1] for col in cursor.fetchall()]
        self.assertIn("id", columns)
        self.assertIn("value", columns)
        self.assertIn("set_size", columns)

    def test_insert_int(self):
        create_table(self.conn, "duplicates_int")
        insert_values(self.conn, "duplicates_int", 1000)
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM duplicates_int")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1000)

    def test_insert_float(self):
        create_table(self.conn, "duplicates_float")
        insert_values(self.conn, "duplicates_float", 10_000)
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM duplicates_float")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 10_000)

    def test_generate_duplicate_values_int(self):
        count = 1000
        scope = 5000

        values_int = generate_duplicate_values("duplicates_int", count, scope)
        self.assertEqual(len(values_int), count)
        vals_int_only = [val for val, _ in values_int]
        unique_vals_int = set(vals_int_only)
        self.assertGreater(len(unique_vals_int), int(count * 0.2))
        self.assertLessEqual(len(unique_vals_int), count)
        for val, set_size in values_int:
            self.assertIsInstance(val, int)
            self.assertGreaterEqual(val, 0)
            self.assertLessEqual(val, scope)
            self.assertEqual(set_size, count)

    def test_generate_duplicate_values_float(self):
        count = 1000
        scope = 5000

        values_float = generate_duplicate_values("duplicates_float", count, scope)
        self.assertEqual(len(values_float), count)
        vals_float_only = [val for val, _ in values_float]
        unique_vals_float = set(vals_float_only)
        self.assertGreater(len(unique_vals_float), int(count * 0.2))
        self.assertLessEqual(len(unique_vals_float), count)
        for val, set_size in values_float:
            self.assertIsInstance(val, float)
            self.assertGreaterEqual(val, 0)
            self.assertLessEqual(val, scope)
            self.assertEqual(set_size, count)

    def test_generate_data_wrong_table_name(self):
        with self.assertRaises(ValueError):
            generate_data("wrong_table_name", db_path=self.test_db)

    def test_generate_data_valid_tables(self):
        generate_data("duplicates_int", db_path=self.test_db)
        generate_data("duplicates_float", db_path=self.test_db)

        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM duplicates_int")
        count_int = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM duplicates_float")
        count_float = cursor.fetchone()[0]

        self.assertEqual(count_int, 111000)
        self.assertEqual(count_float, 111000)

        conn.close()

if __name__ == '__main__':
    unittest.main()
