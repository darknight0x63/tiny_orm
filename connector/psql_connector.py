from .abstract_connector import AbstractConnector
from .tools import _conditions_to_sql

import psycopg2


class PsqlConnector(AbstractConnector):
    def __init__(self, username: str, password: str, dbname: str,
                 host: str = 'localhost', port: str = '5432'):
        self.username = username
        self.password = password
        self.dbname = dbname
        self.host = host
        self.port = port
        self.status_message = None
        self.data = None

    def _db_connect(func):
        def wrapper(self, *args, **kwargs):
            query = func(self, *args, **kwargs)
            conn = psycopg2.connect(dbname=self.dbname, user=self.username,
                                    password=self.password, host=self.host,
                                    port=self.port)
            cur = conn.cursor()
            if type(query) == tuple or type(query) == list:
                cur.execute(query[0], query[1])
            else:
                cur.execute(query)
            conn.commit()
            try:
                self.data = cur.fetchall()
            except:
                self.data = None
            cur.close()
            conn.close()

            self.status_message = cur.statusmessage

        return wrapper

    def create_table(self, name: str, values: list):
        self._create_table(name, values)

    def drop_table(self, name: str):
        self._drop_table(name)

    def create(self, table_name: str, values: dict):
        self._create(table_name, values)
        return [it[0] for it in self.data] if self.data else []

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
        return "CREATE TABLE %s(%s);" % (name, _values_str)

    @_db_connect
    def _drop_table(self, name: str):
        return "DROP TABLE IF EXISTS %s" % name

    @_db_connect
    def _create(self, table_name: str, values: dict):
        _columns = ', '.join([key for key in values])
        _values = [values[key] for key in values]
        _values_placeholders = ', '.join(["%s" for key in values])
        return "INSERT INTO %s (%s)" % (table_name, _columns) + "VALUES (%s) RETURNING id;" % _values_placeholders, _values

    @_db_connect
    def _read(self, table_name: str, columns: list = None, conditions: list = []):
        _columns = ", ".join((col for col in columns)) if columns else "*"
        _conditions, _conditions_values = _conditions_to_sql(conditions)
        return "SELECT %s FROM %s %s;" % (_columns, table_name, _conditions), _conditions_values

    @_db_connect
    def _update(self, table_name: str, values: dict, conditions: list = []):
        _columns = ', '.join(["%s=%s" % (key, "%s") for key in values])
        _values = [values[key] for key in values]
        _conditions, _conditions_values = _conditions_to_sql(conditions)
        return "UPDATE %s " % table_name + "SET %s" % _columns + "%s;" % _conditions, _values + _conditions_values

    @_db_connect
    def _delete(self, table_name: str, conditions: list = []):
        _conditions, _conditions_values = _conditions_to_sql(conditions)
        return "DELETE FROM %s %s;" % (table_name, _conditions), _conditions_values

    @_db_connect
    def _execute_raw_sql(self, query):
        return query

