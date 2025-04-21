import configparser
import psycopg2
from sql_queries import count_table_queries

def table_validation(cur, conn):
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