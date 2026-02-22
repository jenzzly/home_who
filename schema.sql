CREATE TABLE IF NOT EXISTS who_life_expectancy (
                id              SERIAL PRIMARY KEY,
                indicator_code  TEXT        NOT NULL,
                country_code    CHAR(3)     NOT NULL,
                continent       TEXT,
                year            SMALLINT    NOT NULL,
                sex             TEXT,                       -- MLE / FMLE / BTSX
                value           NUMERIC(5,2),
                low_value       NUMERIC(5,2),
                high_value      NUMERIC(5,2),
                date_modified   TIMESTAMPTZ,
                loaded_at       TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (country_code, year, sex)
            );

            CREATE TABLE IF NOT EXISTS etl_runs (
                id          SERIAL PRIMARY KEY,
                started_at  TIMESTAMPTZ DEFAULT NOW(),
                finished_at TIMESTAMPTZ,
                rows_loaded INT,
                rows_skipped INT,
                status      TEXT
            );