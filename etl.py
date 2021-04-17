import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries



def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print("Copying data from S3: ", query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print("Transforming data: ", query)
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()