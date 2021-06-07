import models.handlers as handlers
from models.database import Database


class Carteira:

    def __init__(self):
        self.negociacoes = Database.get("select * from negociacoes")

    @staticmethod
    def atualizar_negociacoes():
        df = handlers.atualiza_negociacoes()
        return df

    @staticmethod
    def _preparacao_inicial():
        handlers.carga_inicial()
        handlers.atualiza_negociacoes()
