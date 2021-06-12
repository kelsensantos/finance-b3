# bibliotecas
import pandas as pd
# External modules
from yahooquery import Ticker


class Yahoo:

    def __init__(self, ticker):

        self.ticker = ticker
        self.ticker_sa = ticker + '.SA'
        self.Ticker = Ticker(self.ticker_sa)
        self.nome = self.Ticker.quote_type[self.ticker_sa]['shortName'].replace("  ", " ").replace("  ", " ").replace("  ", " ")
        self.nome_completo = self.Ticker.quote_type[self.ticker_sa]['longName']
        self.preco_ultimo = self.Ticker.summary_detail[self.ticker_sa]['previousClose']
        self.preco_atual = self.Ticker.financial_data[self.ticker_sa]['currentPrice']
        self.splits = self._splits()

    def _splits(self):
        """ Retorna a cotação segundo último fechamento """
        # A variável "errors" consolida eventuais erros em pontos diversos
        history = Ticker(self.ticker_sa).history()
        # Obtem dataframe com splits através do yahooquery
        if 'splits' in history.columns:
            selecao = history['splits'] != 0
            splits = history[selecao]
            splits = splits[['splits']].reset_index()
            # Renomear colunas
            splits.rename(columns={'symbol': 'ticker', 'date': 'data'}, inplace=True)
            splits['ticker'] = splits['ticker'].apply(lambda x: x.replace('.SA', ''))
        else:
            splits = pd.DataFrame()
        return splits
