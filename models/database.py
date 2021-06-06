# bibliotecas
import psycopg2 as pg
# noinspection PyUnresolvedReferences
import chromedriver_binary
# módulos externos
from decouple import config
from sqlalchemy import create_engine


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
