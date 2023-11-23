"""Сервис для построения конвейера машинного обучения."""
from datetime import datetime
import glob
from pathlib import Path
import pickle
from typing import Any, List, Optional

import pandas as pd
from sklearn.linear_model import LogisticRegression

from configs import config


class MLService:

    def __init__(self, model_dir_path: str):
        self.model_dir_path = Path(model_dir_path)

    @classmethod
    def form_dataframe(cls, columns: List[str], data: Any) -> pd.DataFrame:
        """Преобразовываем данные к формату DataFrame."""
        return pd.DataFrame(columns=columns, data=data)

    @classmethod
    def preprocess_data(
        cls,
        dataframe: pd.DataFrame,
        columns_for_delete: Optional[List] = None,
        categorical_columns: Optional[List] = None,
    ) -> pd.DataFrame:
        """Подготовка DataFrame."""
        preprocessed_df: pd.DataFrame = dataframe.copy(deep=True)

        if columns_for_delete:
            preprocessed_df = preprocessed_df.drop(columns=columns_for_delete)

        if categorical_columns:
            for categorical_column in categorical_columns:
                preprocessed_df[categorical_column] = preprocessed_df[categorical_column].astype("category").cat.codes

        return preprocessed_df

    def fit_and_save_model(self, X, y, model=LogisticRegression()):
        """Обучение и сохранение модели."""
        model.fit(X, y)
        file_model_name = f"model_{datetime.now().strftime('%Y%m%d%H%M%S')}.pkl"
        with open(self.model_dir_path / file_model_name, "wb+") as model_file:
            pickle.dump(model, model_file)

    def load_model_and_predict(self, X) -> List[int]:
        """Загрузка модели и предсказание."""
        model_files = glob.glob(self.model_dir_path.as_posix() + "/*.pkl")
        last_model_file_path = Path(list(sorted(model_files, reverse=True))[0])
        with open(last_model_file_path, "rb+") as last_model_file:
            model = pickle.load(last_model_file)
        return model.predict(X)


ml_service = MLService(model_dir_path=config.MODEL_DIR_PATH)
