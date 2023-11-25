"""Описание таблиц для ORM."""
from __future__ import annotations

import os
from typing import Optional

from sqlalchemy import Column, Float, ForeignKey, Integer, MetaData, Text
from sqlalchemy.orm import Mapped, declarative_base

Base = declarative_base(metadata=MetaData(schema=os.environ.get("POSTGRES_DB")))


class Movie(Base):
    """Таблица с фильмами."""
    __tablename__ = "movie"

    id: Mapped[int] = Column(Integer, primary_key=True)
    title: Mapped[str] = Column(Text, nullable=False)
    genres: Mapped[str] = Column(Text, nullable=False)

    def __repr__(self):
        return f"Movie(id={self.id}, title={self.title}, genres={self.genres})"


class MovieImdb(Base):
    """Таблица с фильмами и индексами в IMDB и TMDB."""
    __tablename__ = "movie_imdb"

    id: Mapped[int] = Column(Integer, primary_key=True)
    movie_id: Mapped[int] = Column(Integer, ForeignKey("movie.id", ondelete="cascade"), nullable=False)
    imdb_id: Mapped[int] = Column(Integer, nullable=False)
    tmdb_id: Mapped[Optional[int]] = Column(Integer)

    def __repr__(self):
        return f"MovieImdb(movie_id={self.movie_id}, imdb_id={self.imdb_id}, genres={self.tmdb_id})"


class UserMovieRating(Base):
    """Таблица с рейтингами фильмов от пользователей."""
    __tablename__ = "user_movie_rating"

    id: Mapped[int] = Column(Integer, primary_key=True)
    movie_id: Mapped[int] = Column(Integer, ForeignKey("movie.id", ondelete="cascade"), nullable=False)
    user_id: Mapped[int] = Column(Integer, nullable=False)
    rating: Mapped[float] = Column(Float, nullable=False)
    timestamp: Mapped[int] = Column(Integer, nullable=False)

    def __repr__(self):
        return f"UserMovieRating(movie_id={self.movie_id}, user_id={self.user_id}, rating={self.rating}, " \
               f"timestamp={self.timestamp})"


class UserMovieTag(Base):
    """Таблица с тегами фильмов от пользователей."""
    __tablename__ = "user_movie_tag"

    id: Mapped[int] = Column(Integer, primary_key=True)
    movie_id: Mapped[int] = Column(Integer, ForeignKey("movie.id", ondelete="cascade"), nullable=False)
    user_id: Mapped[int] = Column(Integer, nullable=False)
    tag: Mapped[str] = Column(Text, nullable=False)
    timestamp: Mapped[int] = Column(Integer, nullable=False)

    def __repr__(self):
        return f"UserMovieTag(movie_id={self.movie_id}, user_id={self.user_id}, tag={self.tag}, " \
               f"timestamp={self.timestamp})"
