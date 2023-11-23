CREATE TABLE IF NOT EXISTS titanic (
    id serial PRIMARY KEY,
    survived integer,
    class integer NOT NULL,
    name text NOT NULL,
    sex text NOT NULL,
    age integer,
    sib_sp integer NOT NULL,
    parch integer NOT NULL,
    ticket text NOT NULL,
    fare float NOT NULL,
    cabin text,
    embarked text
)