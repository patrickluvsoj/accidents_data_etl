import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print('Creating table', query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(f"host={config['CLUSTER']['HOST']} \
                            dbname={config['CLUSTER']['DWH_DB']} \
                            user={config['CLUSTER']['DWH_DB_USER']} \
                            password={config['CLUSTER']['DWH_DB_PASSWORD']} \
                            port={config['CLUSTER']['DWH_PORT']}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()