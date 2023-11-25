CREATE TABLE IF NOT EXISTS movie (
    id serial PRIMARY KEY,
    title text NOT NULL,
    genres text NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_imdb (
    id serial PRIMARY KEY,
    movie_id integer NOT NULL,
    imdb_id integer NOT NULL,
    tmdb_id integer,
    FOREIGN KEY (movie_id) REFERENCES movie (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS user_movie_rating (
    id serial PRIMARY KEY,
    movie_id integer NOT NULL,
    user_id integer NOT NULL,
    rating float NOT NULL,
    timestamp integer NOT NULL,
    FOREIGN KEY (movie_id) REFERENCES movie (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS user_movie_tag (
    id serial PRIMARY KEY,
    movie_id integer NOT NULL,
    user_id integer NOT NULL,
    tag text NOT NULL,
    timestamp integer NOT NULL,
    FOREIGN KEY (movie_id) REFERENCES movie (id) ON DELETE CASCADE
);

COPY movie (id, title, genres)
FROM '/var/lib/postgresql/datasets/movies.csv'
WITH CSV DELIMITER ',' HEADER NULL '';

COPY movie_imdb (movie_id, imdb_id, tmdb_id)
FROM '/var/lib/postgresql/datasets/links.csv'
WITH CSV DELIMITER ',' HEADER NULL '';

COPY user_movie_rating (user_id, movie_id, rating, timestamp)
FROM '/var/lib/postgresql/datasets/ratings.csv'
WITH CSV DELIMITER ',' HEADER NULL '';

COPY user_movie_tag (user_id, movie_id, tag, timestamp)
FROM '/var/lib/postgresql/datasets/tags.csv'
WITH CSV DELIMITER ',' HEADER NULL '';
