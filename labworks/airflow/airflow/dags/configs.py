"""Необходимые настройки для сервисов."""
import os


class Config:
    """Класс, содержащий конфигурации с сервисами."""

    # Настройки базы данных
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT"))

    # Настройки для обучения
    TARGET_COLUMN = "survived"
    CSV_DB_COLUMNS = {
        "PassengerId": "id",
        "Survived": "survived",
        "Pclass": "class",
        "Name": "name",
        "Sex": "sex",
        "Age": "age",
        "SibSp": "sib_sp",
        "Parch": "parch",
        "Ticket": "ticket",
        "Fare": "fare",
        "Cabin": "cabin",
        "Embarked": "embarked",
    }
    TABLE_NAME = "titanic"
    MODEL_DIR_PATH = os.getenv("MODEL_DIR_PATH")
    DATASET_PATH = os.getenv("DATASET_PATH")


config = Config()

