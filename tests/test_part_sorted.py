import unittest
import sqlite3
import os

from data_generators.data_generators import (
    create_table,
    insert_values,
    generate_data,
    generate_part_sorted_values
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
            os.remove(self.test_db)

    def test_create_table_int(self):
        create_table(self.conn, "part_sorted_int")
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(part_sorted_int)")
        columns = [col[1] for col in cursor.fetchall()]
        self.assertIn("id", columns)
        self.assertIn("value", columns)
        self.assertIn("set_size", columns)

    def test_create_table_float(self):
        create_table(self.conn, "part_sorted_float")
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(part_sorted_float)")
        columns = [col[1] for col in cursor.fetchall()]
        self.assertIn("id", columns)
        self.assertIn("value", columns)
        self.assertIn("set_size", columns)

    def test_insert_int(self):
        create_table(self.conn, "part_sorted_int")
        insert_values(self.conn, "part_sorted_int", 1000)
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM part_sorted_int")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1000)

    def test_insert_float(self):
        create_table(self.conn, "part_sorted_float")
        insert_values(self.conn, "part_sorted_float", 10_000)
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM part_sorted_float")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 10_000)

    def test_generate_part_sorted_values_int(self):
        count = 1000
        scope = 5000
        parts = 5

        values = generate_part_sorted_values("part_sorted_int", count, scope)
        self.assertEqual(len(values), count)

        vals = [val for val, _ in values]
        set_sizes = [s for _, s in values]
        self.assertTrue(all(s == count for s in set_sizes))
        self.assertTrue(all(isinstance(val, int) for val in vals))
        self.assertTrue(all(0 <= val <= scope for val in vals))

        sorted_count = int(count * 0.4)
        random_count = count - sorted_count
        chunk_size_sorted = sorted_count // parts
        chunk_size_random = random_count // (parts + 1)

        idx = 0
        for i in range(parts):
            # random chunk
            rc = vals[idx:idx + chunk_size_random]
            self.assertTrue(all(0 <= v <= scope for v in rc))
            idx += chunk_size_random

            # sorted chunk
            sc = vals[idx:idx + chunk_size_sorted]
            self.assertEqual(sc, sorted(sc), f"Sorted chunk {i} is not sorted")
            idx += chunk_size_sorted

        # last random chunk
        rc = vals[idx:]
        self.assertTrue(all(0 <= v <= scope for v in rc))

        # The sum of the lengths of all chunks should be equal to count
        self.assertEqual(len(vals), count)

    def test_generate_part_sorted_values_float(self):
        count = 1000
        scope = 5000.0
        parts = 5
        sorted_count = int(count * 0.4)

        values = generate_part_sorted_values("part_sorted_float", count, scope)
        self.assertEqual(len(values), count)

        vals = [val for val, _ in values]
        set_sizes = [s for _, s in values]
        self.assertTrue(all(s == count for s in set_sizes))
        self.assertTrue(all(isinstance(val, float) for val in vals))
        self.assertTrue(all(0 <= val <= scope for val in vals))

        chunk_size_sorted = sorted_count // parts
        chunk_size_random = (count - sorted_count) // (parts + 1)

        idx = 0
        for i in range(parts):
            rc = vals[idx:idx + chunk_size_random]
            self.assertTrue(all(0 <= v <= scope for v in rc))
            idx += chunk_size_random

            sc = vals[idx:idx + chunk_size_sorted]
            self.assertEqual(sc, sorted(sc), f"Sorted chunk {i} is not sorted")
            idx += chunk_size_sorted

        rc = vals[idx:]
        self.assertTrue(all(0 <= v <= scope for v in rc))

        self.assertEqual(len(vals), count)

    def test_generate_data_wrong_table_name(self):
        with self.assertRaises(ValueError):
            generate_data("wrong_table_name", db_path=self.test_db)

    def test_generate_data_valid_tables(self):
        generate_data("part_sorted_int", db_path=self.test_db)
        generate_data("part_sorted_float", db_path=self.test_db)

        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM part_sorted_int")
        count_int = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM part_sorted_float")
        count_float = cursor.fetchone()[0]

        self.assertEqual(count_int, 111000)
        self.assertEqual(count_float, 111000)

        conn.close()

if __name__ == '__main__':
    unittest.main()
