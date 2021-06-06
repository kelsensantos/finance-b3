# bibliotecas
import pandas as pd
# módulos externos
# módulos locais


# Obtém etts na B3 e cria uma classe para uso
class Etfs:
    url = 'http://bvmf.bmfbovespa.com.br/etf/fundo-de-indice.aspx?idioma=pt-br&aba=tabETFsRendaVariavel'
    df = pd.read_html(url)[0].drop('Segmento', axis=1)
    codigos = list(set(df['Código']))
    tickers = list(set((df['Código'] + '11')))

    @staticmethod
    def eh_etf(ticker: str):
        if len(ticker) == 6:
            eh_etf = ticker in Etfs.tickers
            return eh_etf
        else:
            return 'Ticker inválido'

