# External modules
from yahooquery import Ticker
# Local modules
import models.errors as errors


def ultimo_preco(ticker):
    """ Retorna a cotação segundo último fechamento """
    # A variável "errors" consolida eventuais erros em pontos diversos
    ticker_sa = ticker + '.SA'
    try:
        # Obtem o preço através do yahooquery
        preco = Ticker(ticker_sa).summary_detail[ticker_sa]['previousClose']
    # Retorna e trata o erro, caso não funcione
    except Exception as e:
        preco = "Error"
        # a variável "erros" é uma lista de erros que pode compor um dataframe
        errors.add_error('ultimo_preco', e, ticker)
        pass
    return preco


def preco_atual(ticker):
    """ Retorna a cotação segundo último fechamento """
    # A variável "errors" consolida eventuais erros em pontos diversos
    ticker_sa = ticker + '.SA'
    try:
        # Obtem o preço através do yahooquery
        preco = Ticker(ticker_sa).financial_data[ticker_sa]['currentPrice']
    # Retorna e trata o erro, caso não funcione
    except Exception as e:
        preco = "Error"
        # a variável "erros" é uma lista de erros que pode compor um dataframe
        errors.add_erros('ultimo_preco', e, ticker)
        pass
    return preco
