from models.classes import Etfs, Ativo


def tipo_ticker(ticker: str):
    """ Retorna um tipo de acordo com o ticker. Exemplo: 'eft', 'ação', 'fii', etc."""
    # Se listado na classe de etfs, retorna 'etf'
    if Etfs.eh_etf(ticker):
        return "etf"
    else:
        # Casos não retornem as situações anteriores, caso encontrado no Yashoo Finance returna o tipo "ação"
        ativo = Ativo(ticker)
        return ativo.tipo()
