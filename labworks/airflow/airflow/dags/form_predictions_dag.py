"""DAG для предсказания и вставки данных в БД."""
from datetime import datetime, timedelta
import random

from airflow.decorators import dag
from airflow.decorators import task
import pandas as pd

from configs import config
from db_service import db_service
from ml_service import ml_service


@dag(dag_id="form_predictions", start_date=datetime(2022, 1, 1), schedule_interval=timedelta(minutes=1), catchup=False)
def form_predictions():
    @task
    def select_data_for_predictions() -> pd.DataFrame:
        """Выбираются данные из БД."""
        id_rows = random.sample(db_service.get_data(config.TABLE_NAME, columns=["id"]), 5)
        columns = list(config.CSV_DB_COLUMNS.values())
        columns.remove(config.TARGET_COLUMN)
        data = db_service.get_data(config.TABLE_NAME, columns, ids=id_rows)
        return ml_service.form_dataframe(columns, data)

    @task
    def predict_result_and_store_in_db(dataframe: pd.DataFrame) -> None:
        """Загружается модель, и делается предсказание."""
        ids = dataframe["id"].tolist()
        preprocessed_data = ml_service.preprocess_data(
            dataframe,
            columns_for_delete=["name", "ticket", "cabin", "id"],
            categorical_columns=["sex", "embarked"],
        )
        predictions = ml_service.load_model_and_predict(preprocessed_data).tolist()
        for data_id, prediction in zip(ids, predictions):
            db_service.update_data(
                config.TABLE_NAME,
                column=config.TARGET_COLUMN,
                value=prediction,
                data_id=data_id,
            )

    # Описываем конвейер
    selected_data = select_data_for_predictions()
    predict_result_and_store_in_db(selected_data)


form_predictions_dag = form_predictions()
