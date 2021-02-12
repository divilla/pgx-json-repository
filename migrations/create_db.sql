CREATE DATABASE pgxexec_test
    TEMPLATE 'template0'
    ALLOW_CONNECTIONS TRUE
    CONNECTION LIMIT -1
    ENCODING 'UTF8'
    LC_COLLATE 'und-x-icu'
    LC_CTYPE 'und-x-icu';

CREATE USER pgxexec WITH PASSWORD 'pgxexec_test';
GRANT ALL PRIVILEGES ON DATABASE "pgxexec_test" to pgxexec;
GRANT CREATE ON DATABASE "pgxexec_test" TO pgxexec;
