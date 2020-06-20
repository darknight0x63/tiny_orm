from abstract_connector import AbstractConnector

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

    def _dbconnect(func):
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

    @_dbconnect
    def create_table(self, name: str, values: list):
        _values_str = ""
        for idx, val in enumerate(values):
            _values_str += " ".join(val) + (', ' if idx < len(values) - 1 else '')
        return "CREATE TABLE %s(%s);" % (name, _values_str)

    @_dbconnect
    def drop_table(self, name: str):
        return "DROP TABLE IF EXISTS %s" % name

    @_dbconnect
    def create(self, table_name: str, values: dict):
        _columns = ', '.join([key for key in values])
        _values = [values[key] for key in values]
        _values_placeholders = ', '.join(["%s" for key in values])
        return "INSERT INTO %s (%s)" % (table_name, _columns) + "VALUES (%s) RETURNING id;" % _values_placeholders, _values

    @_dbconnect
    def read(self, table_name: str, columns: list = None, conditions: list = []):
        _columns = ", ".join((col for col in columns)) if columns else "*"
        _conditions, _conditions_values = self._conditions_to_sql(conditions)
        return "SELECT %s FROM %s %s;" % (_columns, table_name, _conditions), _conditions_values

    @_dbconnect
    def update(self, table_name: str, values: dict, conditions: list = []):
        _columns = ', '.join(["%s=%s" % (key, "%s") for key in values])
        _values = [values[key] for key in values]
        _conditions, _conditions_values = self._conditions_to_sql(conditions)
        return "UPDATE %s " % table_name + "SET %s" % _columns + "%s;" % _conditions, _values + _conditions_values

    @_dbconnect
    def delete(self, table_name: str, conditions: list = []):
        _conditions, _conditions_values = self._conditions_to_sql(conditions)
        return "DELETE FROM %s %s;" % (table_name, _conditions), _conditions_values

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
