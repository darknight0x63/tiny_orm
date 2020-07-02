from .abstract_connector import AbstractConnector
from .tools import _conditions_to_sql

import sqlite3


class SqliteConnector(AbstractConnector):
    def __init__(self, path):
        self.path = path
        self.data = None

        self._cur = None

    def _db_connect(func):
        def wrapper(self, *args, **kwargs):
            query = func(self, *args, **kwargs)
            conn = sqlite3.connect(self.path)
            cur = conn.cursor()
            cur.execute(query)
            data = cur.fetchall()

            self._cur = cur
            self.data = data if data else None

            conn.commit()
            conn.close()

        return wrapper

    def create_table(self, name: str, values: list):
        self._create_table(name, values)

    def drop_table(self, name: str):
        self._drop_table(name)

    def create(self, table_name: str, values: dict):
        self._create(table_name, values)
        return [self._cur.lastrowid] if self._cur and self._cur.lastrowid else []

    def read(self, table_name: str, columns: list = None, conditions: list = []):
        self._read(table_name, columns, conditions)
        return self.data

    def update(self, table_name: str, values: dict, conditions: list = []):
        self._update(table_name, values, conditions)

    def delete(self, table_name: str, conditions: list = []):
        self._delete(table_name, conditions)

    def execute_raw_sql(self, query: str):
        self._execute_raw_sql(query)
        return self.data

    @_db_connect
    def _create_table(self, name: str, values: list):
        _values_str = ""
        for idx, val in enumerate(values):
            _values_str += " ".join(val) + (', ' if idx < len(values) - 1 else '')
        return "CREATE TABLE IF NOT EXISTS %s(%s)" % (name, _values_str)

    @_db_connect
    def _drop_table(self, name: str):
        return "DROP TABLE IF EXISTS %s" % name

    @_db_connect
    def _create(self, table_name: str, values: dict):
        _columns = ', '.join([key for key in values])
        _values = ', '.join(["'%s'" % values[key] for key in values])
        return "INSERT INTO %s (%s)" % (table_name, _columns) + "VALUES (%s);" % _values

    @_db_connect
    def _read(self, table_name: str, columns: list = None, conditions: list = []):
        _columns = ", ".join((col for col in columns)) if columns else "*"
        _conditions, _conditions_values = _conditions_to_sql(conditions)
        return ("SELECT %s FROM %s %s;" % (_columns, table_name, _conditions)) % tuple("'%s'" % cond_v for cond_v in _conditions_values)

    @_db_connect
    def _update(self, table_name: str, values: dict, conditions: list = []):
        _columns = ', '.join(["%s=%s" % (key, "%s") for key in values])
        _values = [values[key] for key in values]
        _conditions, _conditions_values = _conditions_to_sql(conditions)
        return ("UPDATE %s " % table_name + "SET %s" % _columns + "%s;" % _conditions) % tuple("'%s'" % it for it in _values + _conditions_values)

    @_db_connect
    def _delete(self, table_name: str, conditions: list = []):
        _conditions, _conditions_values = _conditions_to_sql(conditions)
        return "DELETE FROM %s %s;" % (table_name, _conditions) % tuple("'%s'" % cond for cond in _conditions_values)

    @_db_connect
    def _execute_raw_sql(self, query: str):
        return query
