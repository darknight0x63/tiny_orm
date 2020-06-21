from abc import ABC, abstractmethod


class AbstractConnector(ABC):

    @abstractmethod
    def create_table(self, name: str, values: list):
        pass

    @abstractmethod
    def drop_table(self, name: str):
        pass

    @abstractmethod
    def create(self, table_name: str, values: dict):
        pass

    @abstractmethod
    def read(self, table_name: str, columns: list = None, conditions: list = []):
        pass

    @abstractmethod
    def update(self, table_name: str, values: dict, conditions: list = []):
        pass

    @abstractmethod
    def delete(self, table_name: str, conditions: list = []):
        pass

    @abstractmethod
    def execute_raw_sql(self, query: str):
        pass