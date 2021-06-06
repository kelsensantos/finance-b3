"""
Este script cria um crawler para uso com o Portal do Investidor no CEI.
Para uso, importe o objeto "cei" criado ao final.

"""

# Bibliotecas
import time
import pandas as pd
# Módulos externos
from decouple import config
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
# Módulos internos
from models.chromedriver import ChromeDriver


class CrawlerCei:

    def __init__(
            self,
            directory=config('directory'),
            debug=config('DEBUG', cast=bool),
            username=config('b3_username'),
            password=config('b3_password')
    ):

        self.BASE_URL = 'https://ceiapp.b3.com.br/CEI_Responsivo/'
        self.driver = ChromeDriver()
        self.directory = directory
        self.debug = debug
        self.username = username
        self.password = password

        self.__colunas_df_cei = [
            'Data do Negócio',
            'Compra/Venda',
            'Mercado',
            'Prazo/Vencimento',
            'Código Negociação',
            'Especificação do Ativo',
            'Quantidade',
            'Preço (R$)',
            'Valor Total(R$)',
            'Fator de Cotação'
        ]

        self.id_tabela_negociacao_ativos = 'ctl00_ContentPlaceHolder1_rptAgenteBolsa_ctl00_rptContaBolsa_ctl00_pnAtivosNegociados'
        self.id_mensagem_de_aviso = 'CEIMessageDIV'
        self.id_selecao_corretoras = 'ctl00_ContentPlaceHolder1_ddlAgentes'
        self.id_btn_consultar = 'ctl00_ContentPlaceHolder1_btnConsultar'

    def _login(self, direct=False):
        """ Faz login no portal responsivo do CEI B3 """
        # salva printscreen em directory, caso em modo debug
        if self.debug:
            self.driver.save_screenshot(self.directory + r'01.png')
        # acessa página
        if direct:
            self.driver.get(self.BASE_URL)
        # preenche login
        txt_login = self.driver.find_element_by_id('ctl00_ContentPlaceHolder1_txtLogin')
        txt_login.clear()
        txt_login.send_keys(self.username)
        time.sleep(3.0)
        # preenche senha
        txt_senha = self.driver.find_element_by_id('ctl00_ContentPlaceHolder1_txtSenha')
        txt_senha.clear()
        txt_senha.send_keys(self.password)
        time.sleep(3.0)
        # salva printscreen em directory, caso em modo debug
        if self.debug:
            self.driver.save_screenshot(self.directory + r'02.png')
        # clica no botão para logar
        btn_logar = self.driver.find_element_by_id('ctl00_ContentPlaceHolder1_btnLogar')
        btn_logar.click()
        # confirma se deu certo
        try:
            WebDriverWait(self.driver, 60).until(EC.visibility_of_element_located((By.ID, 'objGrafPosiInv')))
        except Exception:
            raise Exception('Nao foi possivel logar no CEI.')
        # salva printscreen em directory, caso em modo debug
        if self.debug:
            self.driver.save_screenshot(self.directory + r'03.png')

    def _converte_trades_para_dataframe(self):
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        top_div = soup.find('div', {'id': self.id_tabela_negociacao_ativos})
        table = top_div.find(lambda tag: tag.name == 'table')
        df = pd.read_html(str(table), decimal=',', thousands='.')[0]
        df = df.dropna(subset=['Mercado'])
        # adiciona corretora ao dataframe
        corretora = soup.find_all('option', selected=True)[0].get_text().split(' ')[2].upper()
        df['corretora'] = corretora
        return df

    def _consulta_e_captura_trades(self, direct=False):
        """ Busca e captura trades no portal do investidor CEI """

        def consultar_click(driver):
            """ Clica no botão 'consultar' """
            ec = EC.element_to_be_clickable((By.ID, self.id_btn_consultar))
            btn_consultar = WebDriverWait(driver, 20).until(ec)
            btn_consultar.click()

        # aguarda elemento existir e estar liberado
        def exists_and_not_disabled(_id):
            def until_fn(driver):
                try:
                    driver.find_element_by_id(_id)
                except NoSuchElementException:
                    return False
                return driver.find_element_by_id(_id).get_attribute("disabled") is None

            return until_fn

        # adiciona modo de acionamento direto do método
        if direct:
            self._login(direct=True)
        # abre página "negociacao-de-ativos"
        self.driver.get(self.BASE_URL + 'negociacao-de-ativos.aspx')
        # salva printscreen em directory, caso em modo debug
        if self.debug: self.driver.save_screenshot(self.directory + r'04.png')
        # cria uma lista para os dataframe nos quais o quadro de cada corretora serão concatenados
        dfs_to_concat = []
        # acessa o painel de seleção
        ddl_agentes = Select(self.driver.find_element_by_id(self.id_selecao_corretoras))
        # seleciona cada corretora e busca dataframes
        if len(ddl_agentes.options) > 0:
            for i in range(1, len(ddl_agentes.options)):
                ddl_agentes = Select(self.driver.find_element_by_id(self.id_selecao_corretoras))
                # seleciona corretora
                ddl_agentes.select_by_index(i)
                time.sleep(5)
                # aguarda botão ser habilitado
                WebDriverWait(self.driver, 15).until(exists_and_not_disabled(self.id_btn_consultar))
                # clica botão consultar
                consultar_click(self.driver)
                try:
                    # verifica se subiu aviso de que não há trades
                    condition_id = self.id_mensagem_de_aviso
                    WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.ID, condition_id)))
                except Exception:
                    pass
                # lê aviso
                aviso = self.driver.find_element_by_id(self.id_mensagem_de_aviso)
                if aviso.text == 'Não foram encontrados resultados para esta pesquisa.\n×':
                    self.driver.get(self.BASE_URL + 'negociacao-de-ativos.aspx')
                    WebDriverWait(self.driver, 60).until(exists_and_not_disabled(self.id_selecao_corretoras))
                else:
                    condition_id = self.id_tabela_negociacao_ativos
                    WebDriverWait(self.driver, 30).until(EC.visibility_of_element_located((By.ID, condition_id)))
                    dfs_to_concat.append(self._converte_trades_para_dataframe())
                    self.driver.get(self.BASE_URL + 'negociacao-de-ativos.aspx')
                    WebDriverWait(self.driver, 60).until(exists_and_not_disabled(self.id_selecao_corretoras))
        else:
            return print('Não foram encontrdas corretoras.')
        # retorna um dataframe com os dados coletados
        if len(dfs_to_concat):
            return pd.concat(dfs_to_concat)
        # retorna somente labels das colunas em um dataframe zerado, caso não encontre
        else:
            return pd.DataFrame(columns=self.__colunas_df_cei)

    @staticmethod
    def _converte_dataframe_para_formato_padrao(df):

        # renomeia colunas
        df = df.rename(columns={
            'Código Negociação': 'ticker',
            'Compra/Venda': 'operacao',
            'Quantidade': 'qtd',
            'Data do Negócio': 'data',
            'Preço (R$)': 'preco',
            'Valor Total(R$)': 'valor_total'
        }
        )

        def remove_fracionado_ticker(ticker):
            return ticker[:-1] if ticker.endswith('F') else ticker

        def normaliza_operacao(row):
            return row['qtd'] if row['operacao'] == 'C' else (row['qtd'] * -1)

        def normaliza_valor(row):
            return row['valor_total'] if row['operacao'] == 'C' else (row['valor_total'] * -1)

        # corrige tipo de dados
        df['valor_total'] = df['valor_total'].astype(float)
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)
        df['data'] = df['data'].dt.date
        # corrige formato de dados
        df['ticker'] = df.apply(lambda row: remove_fracionado_ticker(row.ticker), axis=1)
        # acrescenta novas colunas
        df['qtd_ajustada'] = df.apply(lambda row: normaliza_operacao(row), axis=1)
        df['valor_ajustado'] = df.apply(lambda row: normaliza_valor(row), axis=1)
        df['taxas'] = 0
        df['aquisicao_via'] = 'home brocker'
        # organiza valores por data
        df.sort_values(by='data', inplace=True)
        # elimina colunas excessivas
        df.drop(
            columns=[
                'Mercado',
                'Prazo/Vencimento',
                'Especificação do Ativo',
                'Fator de Cotação'
            ],
            inplace=True
        )
        return df

    def negociacoes_recentes(self):
        """ Busca trades da B3 em crawler no seu portal responsivo.
            Atenção: o extrato do CEI-B3 é limitado no tempo, ou seja, não serão retornadas todas as operações.
            O extrato também não contém histórico de subscrições. """
        try:
            self.driver.get(self.BASE_URL)
            self._login()
            df = self._consulta_e_captura_trades()
            df = self._converte_dataframe_para_formato_padrao(df)
            return df
        finally:
            self.driver.quit()
