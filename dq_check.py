import configparser
import psycopg2
from sql_queries import count_table_queries

def table_validation(cur, conn):
    """
    This function loops through the count_table_queries list imported from sql_queries.py,
    returning row counts for all fact and dimension tables in the Redshift database.

    Args:
        cur (class): psycopg2 cursor for db interaction
        conn (class): psycopg2 db connection session
    """

    print("Validating table row counts...")
    for query in count_table_queries:
        cur.execute(query)
        conn.commit()
        print(query + " = " + str(cur.fetchone()[0]) + "\n")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    table_validation(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()