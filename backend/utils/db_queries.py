keepalive_kwargs = {
    "keepalives": 1,
    "keepalives_idle": 60,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}
CONNECTION = "postgres://tsdbadmin:em7pj9cc9zahabj7@ir7rkd8932.xc2jrfnbq7.tsdb.forge.timescale.com:34075/tsdb?sslmode=require"

create_table_query = """
            CREATE TABLE {SYMBOL} (
                token   VARCHAR(10) NOT NULL,
                time    TIMESTAMP NOT NULL,
                open    DOUBLE PRECISION,
                high    DOUBLE PRECISION,
                low     DOUBLE PRECISION,
                close   DOUBLE PRECISION,
                volume  DOUBLE PRECISION
            );
"""
create_hypertable_query = """        
    SELECT create_hypertable('{SYMBOL}', 'time');
"""
delete_table_query = "DROP TABLE {SYMBOL};"
