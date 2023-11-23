"""Модуль для работы с базой данных."""
from typing import Any, List, Optional, Tuple

import psycopg2

from configs import config


class DBService:
    """Класс управления БД."""

    def __init__(self, database: str, user: str, password: str, host: str, port: int):
        self.__connection = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)

    def insert_data(
        self,
        table_name: str,
        columns: List[str],
        values: List[Any],
    ) -> None:
        """Вставка данных в таблицу."""
        values_for_insert = []
        columns_for_insert = []
        for value, column in zip(values, columns):
            if str(value) not in ("None", "nan", "NaN"):
                values_for_insert.append(value)
                columns_for_insert.append(column)
        str_columns = ", ".join(columns_for_insert)
        format_strings = ", ".join(["%s" for _ in values_for_insert])
        with self.__connection.cursor() as cursor:
            query = f"INSERT INTO {table_name} ({str_columns}) VALUES ({format_strings})"
            cursor.execute(query, values_for_insert)
            self.__connection.commit()

    def update_data(self, table_name: str, column: str, value: Any, data_id: int) -> None:
        """Обновление данных в таблице."""
        query = f"UPDATE {table_name} SET {column} = {value} WHERE id = {data_id}"
        with self.__connection.cursor() as cursor:
            cursor.execute(query)
            self.__connection.commit()

    def get_data(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        ids: Optional[List[Tuple[int]]] = None,
    ) -> List[Tuple[Any]]:
        """Выборка данных из таблицы в БД."""
        query = f"SELECT {', '.join(columns) or '*'} FROM {table_name}"
        if ids:
            query += f" WHERE id IN ({', '.join([str(item[0]) for item in ids])})"
        with self.__connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()


db_service = DBService(
    database=config.POSTGRES_DB,
    user=config.POSTGRES_USER,
    password=config.POSTGRES_PASSWORD,
    host=config.POSTGRES_HOST,
    port=config.POSTGRES_PORT,
)
