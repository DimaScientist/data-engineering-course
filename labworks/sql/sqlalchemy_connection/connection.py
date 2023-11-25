"""Модуль для установки соединения для SQLAlchemy."""
from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Final, Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

SQLALCHEMY_CONNECTION_URL: Final = f"postgresql://" \
                                   f"{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASSWORD')}@" \
                                   f"{os.environ.get('POSTGRES_HOST')}:{os.environ.get('POSTGRES_PORT')}/" \
                                   f"{os.environ.get('POSTGRES_DB')}"

engine = create_engine(SQLALCHEMY_CONNECTION_URL)
session = scoped_session(sessionmaker(bind=engine))


def get_session() -> Iterator[scoped_session]:
    """Генератор выдачи сессии."""
    try:
        yield session
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.remove()
