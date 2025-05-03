import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    This function loops through the drop_table_queries list imported from sql_queries.py,
    dropping all tables if they exist in the Redshift database.

    Args:
        cur (class): psycopg2 cursor for db interaction
        conn (class): psycopg2 db connection session
    """
    
    print("Dropping existing tables...")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("Existing tables dropped! \n")


def create_tables(cur, conn):
    """
    This function loops through the create_table_queries list imported from sql_queries.py,
    creating the staging, fact, and dimension tables in the Redshift database.

    Args:
        cur (class): psycopg2 cursor for db interaction
        conn (class): psycopg2 db connection session
    """

    print("Creating new tables...")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("New tables created!")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()