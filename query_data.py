import sqlite3
import pandas as pd
import argparse


def query_by_date_and_type(db_path: str) -> pd.DataFrame:
    connection = sqlite3.connect(db_path)

    query = '''
        SELECT DATE(created_at) AS data, tipo, nome_tipo,
               COUNT(*) AS quantidade
        FROM dados_finais
        GROUP BY DATE(created_at), tipo, nome_tipo
        ORDER BY data, tipo, nome_tipo
    '''

    df = pd.read_sql_query(query, connection)
    connection.close()
    print(df)

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Receive a db path and make a \
                                      query based on date and type.")

    parser.add_argument("db_path", metavar="db_path", type=str,
                        help="Path to the db.")

    args = parser.parse_args()

    query_by_date_and_type(args.db_path)
