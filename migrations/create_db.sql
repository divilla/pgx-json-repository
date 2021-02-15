CREATE DATABASE pgxjrep_test
    TEMPLATE 'template0'
    ALLOW_CONNECTIONS TRUE
    CONNECTION LIMIT -1
    ENCODING 'UTF8'
    LC_COLLATE 'und-x-icu'
    LC_CTYPE 'und-x-icu';

CREATE USER pgxjrep WITH PASSWORD 'pass';
GRANT ALL PRIVILEGES ON DATABASE "pgxjrep_test" to pgxjrep;
GRANT CREATE ON DATABASE "pgxjrep_test" TO pgxjrep;
