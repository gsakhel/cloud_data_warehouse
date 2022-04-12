import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


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
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    table_query(cur, conn, copy_table_queries)
    table_query(cur, conn, insert_table_queries)

    conn.close()


if __name__ == "__main__":
    main()



# def load_staging_tables(cur, conn):
#     for query in copy_table_queries:
#         cur.execute(query)
#         conn.commit()


# def insert_tables(cur, conn):
#     for query in insert_table_queries:
#         cur.execute(query)
#         conn.commit()


# def main():
#     config = configparser.ConfigParser()
#     config.read('dwh.cfg')

#     conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
#     cur = conn.cursor()
    
#     load_staging_tables(cur, conn)
#     insert_tables(cur, conn)

#     conn.close()


# if __name__ == "__main__":
#     main()