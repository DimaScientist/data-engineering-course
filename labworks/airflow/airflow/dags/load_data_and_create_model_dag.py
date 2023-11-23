"""DAG для загрузки данных и обучения модели."""
from datetime import datetime

import pandas as pd
from airflow.decorators import dag
from airflow.decorators import task

from configs import config
from db_service import db_service
from ml_service import ml_service


@dag(dag_id="load_data_and_create_model", start_date=datetime(2022, 1, 1), schedule_interval=None, catchup=False)
def load_data_and_create_model():
    @task
    def load_data_from_csv_file() -> pd.DataFrame:
        """Загружаются данные из CSV-файла."""
        data = pd.read_csv(config.DATASET_PATH).sample(frac=1).rename(columns=config.CSV_DB_COLUMNS)
        if int(data.isnull().sum().sum()) > 0:
            data = data.dropna()
        return data

    @task
    def load_data_in_db(data: pd.DataFrame) -> None:
        """Загрузка данных в БД."""
        data_without_target = data.iloc[int(0.7 * len(data)):].drop(columns=[config.TARGET_COLUMN])
        columns_for_insert = list(config.CSV_DB_COLUMNS.values())
        columns_for_insert.remove(config.TARGET_COLUMN)
        for values in data_without_target.values.tolist():
            db_service.insert_data(config.TABLE_NAME, columns=columns_for_insert, values=values)

    @task
    def train_and_save_model(data: pd.DataFrame) -> None:
        """Тренировка и сохранение модели."""
        preprocessed_data = ml_service.preprocess_data(
            data.iloc[:int(0.7 * len(data))],
            columns_for_delete=["name", "ticket", "cabin", "id"],
            categorical_columns=["sex", "embarked"],
        )
        X = preprocessed_data.drop(columns=[config.TARGET_COLUMN])
        y = preprocessed_data[config.TARGET_COLUMN]
        ml_service.fit_and_save_model(X, y)

    # Описываем конвейер
    data = load_data_from_csv_file()
    load_data_in_db(data)
    train_and_save_model(data)


load_data_and_create_model_dag = load_data_and_create_model()
