# bibliotecas
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
    def atualizar_database(df, tablename):
        """ Grava um dataframe no banco de dados. """
        # grava o dataframe no banco de dados
        engine = Database.engine()
        df.to_sql(
            name=tablename,
            con=engine,
            if_exists='replace',
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
