import unittest
import random
from datetime import datetime
import os

from connector.sqlite_connector import SqliteConnector

SCHEMA = [
    ('id', 'INTEGER', 'PRIMARY KEY', 'AUTOINCREMENT'),
    ('name', 'TEXT'),
    ('test_text', 'TEXT'),
    ('test_integer', 'INTEGER'),
    ('test_float', 'REAL'),
    ('test_date', 'TEXT'),
    ('test_timestamp', 'TEXT')
]

LETTERS_POOL = 'abcdefghijklmnopqrstuvwxyz'

DATA = []
random_string = lambda pool, length: ''.join((random.choice(pool) for i in range(length)))
random_int = lambda begin, end: random.randint(begin, end)
random_float = lambda begin, end: random.uniform(begin, end)

for i in range(0, 100):
    DATA.append({
        'name': random_string(LETTERS_POOL, random_int(1, 99)),
        'test_text': random_string(LETTERS_POOL, random_int(1, 1000)),
        'test_integer': random_int(1, 1000),
        'test_float': random_float(1, 1000),
        'test_date': str(datetime.now().date()),
        'test_timestamp': str(datetime.now())
    })


class TestDropTable(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = SqliteConnector(path="./test.db")

    def test_drop_table(self):
        self.conn.drop_table("test_abc")

    @classmethod
    def tearDownClass(cls):
        os.remove("./test.db")


class TestCreateTable(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = SqliteConnector(path="./test.db")
        cls.conn.drop_table("test_abc")

    def test_create_table(self):
        self.conn.create_table("test_abc", SCHEMA)

    @classmethod
    def tearDownClass(cls):
        os.remove("./test.db")


class TestNoErrorsCRUD(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = SqliteConnector(path="./test.db")
        cls.conn.drop_table("test_abc")
        cls.conn.create_table("test_abc", SCHEMA)

    def test_create_records(self):
        for it in DATA:
            self.conn.create('test_abc', it)

    def test_read_records(self):
        self.conn.read('test_abc')
        self.conn.read('test_abc',
                       conditions=[('id', '=', 1)])
        self.conn.read('test_abc',
                       conditions=[('test_text', '=', DATA[0]['test_text']), '&', ('name', '=', DATA[0]['name'])])
        self.conn.read('test_abc',
                       columns=['id', 'name', 'test_text'])
        self.conn.read('test_abc',
                       columns=['id', 'name', 'test_text'],
                       conditions=[('test_text', '=', DATA[0]['test_text']), '&', ('name', '=', DATA[0]['name'])])
        self.conn.read('test_abc',
                       columns=['id', 'name', 'test_text'],
                       conditions=[('id', 'in', [1, 2, 3, 5])])
        self.conn.read('test_abc',
                       columns=['id', 'name', 'test_text'],
                       conditions=[('id', 'not in', [1, 2, 3, 5])])

    def test_update_records(self):
        self.conn.update('test_abc',
                         values={'name': 'new_name', 'test_date': '1800-01-01'})
        self.conn.update('test_abc',
                         values={'name': 'new_name_123', 'test_date': '1700-01-01'},
                         conditions=[('id', '=', 1), '|', ('id', '=', 2)])

    def test_delete_records(self):
        self.conn.delete('test_abc',
                         conditions=[('id', '=', 1)])

        self.conn.delete('test_abc',
                         conditions=[('name', '=', DATA[0]['name'])])

    def test_raw_sql(self):
        self.conn.execute_raw_sql("SELECT * FROM test_abc;")
        self.conn.execute_raw_sql("SELECT * FROM test_abc WHERE id=1;")
        self.conn.execute_raw_sql("INSERT INTO test_abc (name, test_text) VALUES ('xyz', 'abc');")

    @classmethod
    def tearDownClass(cls):
        os.remove("./test.db")
