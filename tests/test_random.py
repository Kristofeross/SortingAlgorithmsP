import unittest
import  sqlite3
import os

from data_generators.random_data import create_table, generate_insert_values, generate_random

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
        create_table(self.conn, "random_int")
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(random_int)")
        columns = [col[1] for col in cursor.fetchall()]
        self.assertIn("id", columns)
        self.assertIn("value", columns)
        self.assertIn("set_size", columns)

    def test_create_table_float(self):
        create_table(self.conn, "random_float")
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(random_float)")
        columns = [col[1] for col in cursor.fetchall()]
        self.assertIn("id", columns)
        self.assertIn("value", columns)
        self.assertIn("set_size", columns)

    def test_generate_insert_int(self):
        create_table(self.conn, "random_int")
        generate_insert_values(self.conn, "random_int", 1000)
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM random_int")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1000)

    def test_generate_insert_float(self):
        create_table(self.conn, "random_float")
        generate_insert_values(self.conn, "random_float", 10000)
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM random_float")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 10000)

    def test_generate_random_invalid_table(self):
        with self.assertRaises(ValueError):
            generate_random("wrong_table_name", db_path=self.test_db)

    def test_generate_random_valid_tables(self):
        generate_random("random_int", db_path=self.test_db)
        generate_random("random_float", db_path=self.test_db)
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM random_int")
        count_int = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM random_float")
        count_float = cursor.fetchone()[0]
        self.assertEqual(count_int, 111000)
        self.assertEqual(count_float, 111000)
        conn.close()

if __name__ == '__main__':
    unittest.main()
