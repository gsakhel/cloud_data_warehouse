import configparser
from venv import create
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def table_query(cur, conn, table_query_list : list):
    """Run a list of SQL queries
    
    Args:
        cur (psycopg2.extensions.cursor:     Postgres cursor
        conn (psycopg2.exensions.connectoin: Postgres connection
        table_query_list (list):             List of SQL queries
        
    """
    for query in table_query_list:
        cur.execute(query)
        conn.commit()

def main():
    """Connect to Redshift database, drop and create tables"""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    table_query(cur, conn, drop_table_queries)
    table_query(cur, conn, create_table_queries)

    conn.close()


if __name__ == "__main__":
    main()