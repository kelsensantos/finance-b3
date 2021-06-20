# bibliotecas
# import pandas
import pandas as pd
import psycopg2 as pg
# noinspection PyUnresolvedReferences
import chromedriver_binary
# módulos externos
from decouple import config
from sqlalchemy import create_engine
from datetime import datetime


# Classe utilizada para operação de banco de dados
class Database:

    host = config('DB_HOST')
    name = config('DB_NAME')
    user = config('DB_USER')
    password = config('DB_PASS')
    port = config('DB_PORT')

    @staticmethod
    def connection(host=host, name=name, user=user, password=password):
        connection = pg.connect(host=host, database=name, user=user, password=password)
        return connection

    @staticmethod
    def engine(host=host, name=name, user=user, password=password, port=port):
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{name}')
        return engine

    @staticmethod
    def get(sql, host=host, name=name, user=user, password=password):
        connection = pg.connect(host=host, database=name, user=user, password=password)
        df = pd.read_sql(sql, con=connection)
        return df

    @staticmethod
    def get_table(tablename, host=host, name=name, user=user, password=password):
        connection = pg.connect(host=host, database=name, user=user, password=password)
        sql = f'select * from {tablename}'
        df = pd.read_sql(sql, con=connection)
        return df

    @staticmethod
    def atualizar_database(df, tablename, if_exists='replace'):
        """ Grava um dataframe no banco de dados. """
        # grava o dataframe no banco de dados
        engine = Database.engine()
        df.to_sql(
            name=tablename,
            con=engine,
            if_exists=if_exists,
            index=False
        )
        # salva cópia de segurança
        filename = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
        df.to_csv(
            f"output/security_copy_{tablename}_{filename}.csv",
            sep=';',
            index=False
        )
        engine.dispose()
        return df

    @staticmethod
    def insert_df(df, tablename, if_exists='append'):
        """ Grava um dataframe no banco de dados. """
        # grava o dataframe no banco de dados
        engine = Database.engine()
        df.to_sql(
            name=tablename,
            con=engine,
            if_exists=if_exists,
            index=False
        )
        # salva cópia de segurança
        Database.fazer_backup(df=df, tablename=tablename)
        engine.dispose()
        return df

    @staticmethod
    def insert_value(tablename, coluna, ticker):
        Database.execute(f"INSERT INTO {tablename}({coluna}) values('{ticker}')")

    @staticmethod
    def fazer_backup(df, tablename):
        filename = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
        df.to_csv(
            f"output/security_copy_{tablename}_{filename}.csv",
            sep=';',
            index=False
        )

    @staticmethod
    def verificar_existencia_de_tabela(tablename):
        sql = f"SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_name = '{tablename}');"
        engine = Database.engine()
        connection = engine.connect()
        result = connection.execute(sql).fetchall()[0][0]
        connection.close()
        engine.dispose()
        return result

    @staticmethod
    def execute(sql):
        engine = Database.engine()
        connection = engine.connect()
        connection.execute(sql)
        connection.close()
        engine.dispose()
