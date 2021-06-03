# bibliotecas
import pandas as pd
# noinspection PyUnresolvedReferences
import chromedriver_binary  # do not remove
# módulos externos
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
# módulos locais
import models.errors as errors


def _busca_etfs_na_b3():
    # URL de onde obter lista de ETF's da B3
    url = 'http://bvmf.bmfbovespa.com.br/etf/fundo-de-indice.aspx?idioma=pt-br&aba=tabETFsRendaVariavel'
    df = pd.read_html(url)
    efts_df = df[0]
    efts_df.drop('Segmento', axis=1, inplace=True)
    efts_df['Ticker'] = efts_df['Código'] + '11'
    return efts_df


# Obtém etts na B3 e cria uma classe para uso
class Etfs:

    df = _busca_etfs_na_b3()
    lista = list(set(df['Código']))
    tickers = list(set(df['Ticker']))

    @staticmethod
    def eh_etf(ticker: str):
        x = ticker.replace('11', '').upper()
        eh_etf = x in Etfs.lista
        return eh_etf


# Driver para uso do Selenium
class ChromeDriver(object):

    def __new__(cls):
        return ChromeDriver.__configure_driver(headless=True)

    @staticmethod
    def __configure_driver(headless):
        # configurações do Driver
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        if headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        # Cria o driver com Selenium
        driver = webdriver.Chrome(options=chrome_options)
        driver.implicitly_wait(20)
        return driver


class Ativo:

    def __init__(self, ticker):

        # função para atribuir URL no site br.advfn.com ao ticker
        def _set_url(ticker):
            return f'https://br.advfn.com/bolsa-de-valores/bmf/{ticker}/cotacao'

        self.driver = ChromeDriver()
        self.ticker = ticker
        self.url = _set_url(ticker)

    def info(self):
        """ Retorna um dicionário com tipo e preço atual do ativo """
        return {'tipo': self.tipo(), 'preco_atual': self.preco_atual()}

    # busca informações de preço atual com crawler no site br.advfn.com
    def preco_atual(self):
        try:
            # crawler no site br.advfn.com
            self.driver.get(self.url)
            ec = EC.visibility_of_element_located((By.ID, 'quoteElementPiece10'))
            WebDriverWait(self.driver, 10).until(ec)
            x = self.driver.find_element_by_id('quoteElementPiece10')
            x = x.text.replace('.', '').replace(',', '.')
            preco_atual = float(x)
            return preco_atual
        # trata erros adicionando informações em 'errors'
        except Exception as e:
            errors.add_error('Ativo.preco_atual', e, self.ticker)
            return None

    # atribui tipo do ativo com crawler no site br.advfn.com
    def tipo(self):
        try:
            # crawler no site br.advfn.com
            self.driver.get(self.url)
            ec = EC.visibility_of_element_located((By.ID, 'quoteElementPiece5'))
            WebDriverWait(self.driver, 10).until(ec)
            tipo = self.driver.find_element_by_id('quoteElementPiece5').text.lower()
            codigo_isin = self.driver.find_element_by_id('quoteElementPiece6').text.lower()
            # trata as informações obtidas nos crawlers definindo tipo
            if tipo == 'preferencial' or tipo == 'ordinária':
                return 'ação'
            if tipo == 'fundo':
                if self.eh_fii():
                    return 'fii'
                if self.eh_etf():
                    return 'etf'
            if (tipo == 'recibo de depósito') and ('bdr' in codigo_isin):
                return 'bdr'
            return tipo
        # trata erros adicionando informações em 'errors'
        except Exception as e:
            errors.add_error('Ativo._tipo', e, self.ticker)
            return None

    def eh_etf(self):
        """ A partir de crawler em br.advfn.com verifica se é eft """
        try:
            self.driver.get(self.url)
            if 'Exchange Traded Fund' in self.driver.page_source:
                return True
            return False
        # trata erro, caso aconteça
        except Exception as e:
            errors.add_error('Ativo.eh_fii', e, self.ticker)
        return None

    def eh_fii(self):
        """ A partir de crawler em br.advfn.com verifica se é fii """
        try:
            self.driver.get(self.url)
            if 'FII' in self._nome() and len(self.dividendos()):
                return True
            return False
        except Exception as e:
            errors.add_error('Ativo.eh_fii', e, self.ticker)
        return None

    def _nome(self):
        """ Retorna o nome do ativo em br.advfn.com """
        self.driver.get(self.url)
        nome = self.driver.find_elements_by_class_name("page-name-h1")[0].text.upper()
        return nome

    def dividendos(self):
        try:
            self.driver.get(self.url)
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            table = soup.find('table', {'id': 'id_stocks_dividends'})
            df = pd.read_html(str(table), decimal=',', thousands='.')[0]
            return df
        except Exception:
            return None
