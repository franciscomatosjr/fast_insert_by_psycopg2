import psycopg2
from psycopg2 import extras
import pandas as pd
import datetime
from io import StringIO

# ref: https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/


class banco_de_dados():


    def __init__(self):

        self.user = ""
        self.password = ""
        self.port = 5432
        self.database = ""
        self.host = ""
        self.dialect = "postgresql"
        self.driver = "psycopg2"
        self.schema = "Public"
        self.conn = self.get_db_connector()
        self.cur = None

    # def __enter__(self):
    #     self.conn = self.get_db_connector()
    #
    # def __exit__(self, exc_type = None, exc_val = None, exc_tb = None):
    #     self.conn.close()

    def get_db_connector(self):
        self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port, options=f'-c search_path="{self.schema}"')
        return self.conn

    def get_db_cursor(self):
        if self.conn is None:
            self.get_db_connector()

        if self.cur is None:
            self.cur = self.get_db_connector().cursor()
        return self.cur

    def close_db_connection(self):
        conn = self.get_db_cursor()

        self.cur.close(), self.conn.close()

    def persistir_dados_copy_expert(self, df: pd.DataFrame, table_name: str):
        try:
            schema = "FonteExterna"
            table = f'"{schema}"."{table_name}"'
            sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ',' "
            df.to_csv(table_name+'.csv', sep=',', index=False)
            file = open(table_name+'.csv', 'r')

            with self.conn:

                cur = self.conn.cursor()

                cur.copy_expert(sql=sql % table, file=file)

                self.conn.commit()
                self.cur.close()
                self.conn.close()
                print("Dados persistidos com sucesso!")

            return True
        except Exception as erro:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            print(erro)
            raise erro

    def persistir_dados_copy_from_stringio(self, df, table):
        """
        Here we are going save the dataframe in memory
        and use copy_from() to copy it to the table
        """
        # save dataframe to an in memory buffer

        buffer = StringIO()
        # df.to_csv(buffer, index_label='id', header=False)
        df.to_csv(buffer, header=False, index=False)
        buffer.seek(0)

        cursor = self.conn.cursor()
        # cursor.execute("SET search_path TO " + self.schema)

        try:
            cursor.copy_from(buffer, table, sep=",")
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1
        print("copy_from_stringio() done")
        cursor.close()

    def persistir_dados_execute_mogrify(self, df, table, update_values: bool = True):
        """
        Using cursor.mogrify() to build the bulk insert query
        then cursor.execute() to execute the query
        """
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]

        # Comma-separated dataframe columns
        cols = '", "'.join(list(df.columns))
        cols = '"' + cols + '"'

        # SQL query to execute
        cursor = self.conn.cursor()
        str_tuple_columns = "({})".format(",".join(["%s" for _ in list(df)]))
        values = [cursor.mogrify(f"{str_tuple_columns}", tup).decode('utf8') for tup in tuples]

        # Query return constraints of table
        qry_primary_key = '''
               SELECT kcu.column_name, tco.constraint_type
                FROM information_schema.table_constraints tco
           LEFT JOIN information_schema.key_column_usage kcu 
                     ON kcu.constraint_name = tco.constraint_name
                     AND kcu.constraint_schema = tco.constraint_schema
                     AND kcu.constraint_name = tco.constraint_name
               WHERE kcu.table_schema = '{}' and KCU.table_name = '{}' and tco.constraint_type = 'PRIMARY KEY'      
        '''.format(self.schema, table)
        cursor.execute(qry_primary_key)

        results_primary_key = cursor.fetchall()

        str_on_conflict = " ON CONFLICT ({})".format(",".join([f'"{_[0]}"' for _ in list(results_primary_key)]))

        if update_values:

            qry_columns_table = '''
            SELECT distinct c.column_name, '' constraint_type
              FROM information_schema.columns c
             WHERE c.table_schema = '{}'
               AND c.table_name   = '{}'	
            '''.format(self.schema, table)

            cursor.execute(qry_columns_table)

            results_columns_table = cursor.fetchall()

            str_update_set = " DO UPDATE SET {}".format(", ".join([f'"{column[0]}" = EXCLUDED."{column[0]}"' for column in list(results_columns_table) if column not in dict(results_primary_key).keys()]))

            query = "INSERT INTO %s(%s) VALUES " % (f'"{self.schema}"."{table}"', cols) + ",".join(values) + f' {str_on_conflict} {str_update_set}'
        else:
            # dont update values if exists in database
            query = "INSERT INTO %s(%s) VALUES " % (f'"{self.schema}"."{table}"', cols) + ",".join(values) + f' {str_on_conflict} DO NOTHING'

        try:
            cursor.execute(query, tuples)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1
        print("execute_mogrify() done")
        cursor.close()

    def persistir_dados_batch(self, df: pd.DataFrame, table : str):
        if len(df) > 0:
            df_columns = list(df)

            # create (col1, col2, ...)
            columns = ", ".join(df_columns)

            # create VALUES (%S, %S, ...) ONE '%s'per columns
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

            # create INSERT INTO table (columns) VALUES ('%s', ...)
            insert_stmt = "INSERT {} ({}) {}".format(table, columns, values)

            cur = self.get_db_cursor()

            psycopg2.extras.execute_batch(cur, insert_stmt, df.values, page_size=1000)


if __name__ == "__main__":
    banco = banco_de_dados()

    data_df = pd.DataFrame([{"CodigoRFBPais": 1312, "CodigoRFBRFBPais": 9975, "NomeRFBPais": "PaisTeste", "DataHoraPublicacao": datetime.datetime.now(), "DataHoraInsercao": datetime.datetime.now()}])
    # data_df = pd.DataFrame([{"CodigoRFBPais": 1312, "CodigoRFBRFBPais": 9975, "NomeRFBPais": "PaisTeste"}])

    # status = banco.persistir_dados_copy_expert(data_df, "RFBPais")
    # status = banco.persistir_dados_copy_from_stringio(data_df, "RFBPais")
    status = banco.persistir_dados_execute_mogrify(data_df, "RFBPais", update_values=False)

    print(data_df)



