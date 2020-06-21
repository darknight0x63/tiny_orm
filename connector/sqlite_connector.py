from .abstract_connector import AbstractConnector

import sqlite3


class SqliteConnector(AbstractConnector):
    def __init__(self, path):
        self.path = path

    def _db_connect(func):
        def wrapper(self, *args, **kwargs):
            query = func(self, *args, **kwargs)
            conn = sqlite3.connect(self.path)
            cur = conn.cursor()
            cur.execute(query)

        return wrapper

    @_db_connect
    def create_table(self, name: str, values: list):
        _values_str = ""
        for idx, val in enumerate(values):
            _values_str += " ".join(val) + (', ' if idx < len(values) - 1 else '')
        return "CREATE TABLE IF NOT EXISTS %s(%s)" % (name, _values_str)

    @_db_connect
    def drop_table(self, name: str):
        return "DROP TABLE IF EXISTS %s" % name

    @_db_connect
    def create(self, table_name: str, values: dict):
        _columns = ', '.join([key for key in values])
        _values = ', '.join(["'%s'" % values[key] for key in values])
        return "INSERT INTO %s (%s)" % (table_name, _columns) + "VALUES (%s);" % _values

    @_db_connect
    def read(self, table_name: str, columns: list = None, conditions: list = []):
        _columns = ", ".join((col for col in columns)) if columns else "*"
        _conditions, _conditions_values = self._conditions_to_sql(conditions)
        return ("SELECT %s FROM %s %s;" % (_columns, table_name, _conditions)) % tuple("'%s'" % cond_v for cond_v in _conditions_values)

    @_db_connect
    def update(self, table_name: str, values: dict, conditions: list = []):
        _columns = ', '.join(["%s=%s" % (key, "%s") for key in values])
        _values = [values[key] for key in values]
        _conditions, _conditions_values = self._conditions_to_sql(conditions)
        return ("UPDATE %s " % table_name + "SET %s" % _columns + "%s;" % _conditions) % tuple("'%s'" % it for it in _values + _conditions_values)

    @_db_connect
    def delete(self, table_name: str, conditions: list = []):
        _conditions, _conditions_values = self._conditions_to_sql(conditions)
        return "DELETE FROM %s %s;" % (table_name, _conditions) % tuple("'%s'" % cond for cond in _conditions_values)

    @_db_connect
    def execute_raw_sql(self, query: str):
        return query

    @staticmethod
    def _conditions_to_sql(conditions: list) -> str:
        cond_str = str()
        _values = []
        for cond in conditions:
            if type(cond) == str and cond in ["&", "|"]:
                cond_str += " %s" % "AND" if cond == '&' else "OR"
            elif len(cond) == 3:
                cond_str += " %s%s%s" % (cond[0], cond[1], "%s")
                _values.append(cond[2])
            else:
                raise ValueError("Invalid expression %s" % cond)

        if cond_str:
            cond_str = "WHERE " + cond_str

        return cond_str, _values
