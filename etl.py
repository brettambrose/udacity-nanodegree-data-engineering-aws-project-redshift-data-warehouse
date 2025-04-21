import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print("Loading staging tables from S3...")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("Staging tables loaded! \n")


def insert_tables(cur, conn):
    print("Loading Data Warehouse tables...")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print("Data Warehouse tables loaded! \n")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()