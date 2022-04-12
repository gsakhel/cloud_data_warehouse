import configparser
import psycopg2
import pandas as pd
from tabulate import tabulate


def sql_runner(cur, conn, query):
    """Runs an SQL query and returns a DataFrame
    
    Args:
        cur (psycopg2.extensions.cursor:     Postgres cursor
        conn (psycopg2.exensions.connectoin: Postgres connection
        query (string):                      SQL query

    """
    cur.execute(query)
    conn.commit()
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=list(map(lambda x: x[0], cur.description)))
    print(df)
    return df

pd.set_option('display.max_rows',20)

# Grab our configurations
config_file='dwh.cfg'
config=configparser.ConfigParser()
config.read(config_file)

# Create connection to Redshift
conn = psycopg2.connect(dbname=config['CLUSTER']['DB_NAME'],
                        user=config['CLUSTER']['DB_USER'],
                        password=config['CLUSTER']['DB_PASSWORD'],
                        host=config['CLUSTER']['HOST'],
                        port=config['CLUSTER']['DB_PORT'],
                        connect_timeout=5
)
cur = conn.cursor()

# SQL Queries
query1 = """
    SELECT
        DISTINCT sp.song_id AS song_id,
        name AS artist_name,
        title AS song_name,
        count(*) as times_played
    FROM songplay sp
    JOIN artist a
        ON sp.artist_id=a.artist_id
    LEFT JOIN song s
        ON sp.song_id=s.song_id
    GROUP BY sp.song_id, name, title
    ORDER BY sp.song_id DESC
    LIMIT 100;
    """

query2 = """SELECT 
    (SELECT count(*)
    FROM songplay) AS songplay,
    (SELECT count(*) 
    FROM time) AS time,
    (SELECT count(*)
    FROM artist) AS artist_,
    (SELECT count(*)
    FROM users) AS users_,
    (SELECT count(*)
    FROM song) AS song_"""

# Run Queries
sql_runner(cur, conn, query1)
sql_runner(cur, conn, query2)
sql_runner(cur, conn, "SELECT * FROM songplay")