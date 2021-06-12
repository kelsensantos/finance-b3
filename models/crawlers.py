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
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import NoSuchElementException
# Módulos internos
from models.chromedriver import ChromeDriver
from models.database import Database


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


class CrawlerInvesting:

    def __init__(
            self,
            username=config('INVEST_LOGIN'),
            password=config('INVEST_PASSWORD')
    ):

        self.BASE_URL = 'https://br.investing.com/'
        self.username = username
        self.password = password

        self.tablename_desdobramentos = 'desdobramentos'
        self.tablename_splits = "brinvesting_splits"

    def _habilita_driver(self):
        self.driver = ChromeDriver()
        time.sleep(5.0)

    def _login(self, direct=False):
        # habilita uso direto
        if direct:
            self._habilita_driver()
        # acessa página de login
        url = self.BASE_URL + '/login'
        self.driver.get(url)
        time.sleep(5.0)
        # clica no pop-up sobre cookies
        btn_cookies = self.driver.find_element_by_id('onetrust-accept-btn-handler')
        btn_cookies.click()
        time.sleep(5.0)
        # acessa campo de usuário e digita cpf
        txt_username = self.driver.find_element_by_id('loginFormUser_email')
        txt_username.clear()
        txt_username.send_keys(self.username)
        time.sleep(5.0)
        # acessa campo de senha e digita a senha
        txt_password = self.driver.find_element_by_id('loginForm_password')
        txt_password.clear()
        txt_password.send_keys(self.password)
        # localiza e clica no botão de login
        xpath = "//*[@id='signup']/a"
        condition = EC.element_to_be_clickable((By.XPATH, xpath))
        btn = WebDriverWait(self.driver, 20).until(condition)
        btn.click()

    # def _busca_splits(self):
    #
    #     # acessa página de splits
    #     self.driver.get('https://br.investing.com/stock-split-calendar/')
    #     # clica no botão do filtro de calendário
    #     element_id = 'datePickerToggleBtn'
    #     ec = EC.element_to_be_clickable((By.ID, element_id))
    #     btn = WebDriverWait(self.driver, 20).until(ec)
    #     btn.click()
    #     time.sleep(2.0)
    #     # atribui ao campo de início a data de janeiro de 2018
    #     element_id = 'startDate'
    #     start = self.driver.find_element_by_id(element_id)
    #     start.clear()
    #     start.click()
    #     start.send_keys('01/01/2018')
    #     time.sleep(2.0)
    #     # clica no botão para aplicar o filtro de data
    #     element_id = 'applyBtn'
    #     ec = EC.element_to_be_clickable((By.ID, element_id))
    #     btn = WebDriverWait(self.driver, 20).until(ec)
    #     btn.click()
    #     time.sleep(5.0)
    #
    #     def calcula_fator(x):
    #         splitted = x.split(':')
    #         y = float(splitted[0]) // float(splitted[1])
    #         y = round(y, 3)
    #         return y
    #
    #     # prepara html
    #     soup = BeautifulSoup(self.driver.page_source, 'html.parser')
    #     element_id = 'stock-splitCalendarData'
    #     # obtem seleção do html
    #     html = soup.find_all(id=element_id)
    #     # obtém dataframe
    #     df = pd.read_html(str(html))[0]
    #     # corrige nome das colunas
    #     df.columns = ['data', 'ticker', 'fator']
    #     # normaliza ticker
    #     df['ticker'] = df['ticker'].apply(lambda x: x.split('(')[-1])
    #     df['ticker'] = df['ticker'].apply(lambda x: x.replace(')', ''))
    #     # calcula e normaliza fator
    #     df['fator'] = df['fator'].apply(lambda x: calcula_fator(x))
    #     # corrige dypes
    #     df = df.astype({'data': 'datetime64'})
    #     # corrige gaps em data
    #     df.fillna(method='ffill', inplace=True)
    #     return df

    def _busca_e_salva_tabela_de_splits(self):
        # acessa página de splits
        self.driver.get('https://br.investing.com/stock-split-calendar/')
        # clica no botão do filtro de calendário
        element_id = 'datePickerToggleBtn'
        ec = EC.element_to_be_clickable((By.ID, element_id))
        btn = WebDriverWait(self.driver, 20).until(ec)
        btn.click()
        time.sleep(2.0)
        # atribui ao campo de início a data de janeiro de 2018
        element_id = 'startDate'
        start = self.driver.find_element_by_id(element_id)
        start.clear()
        start.click()
        start.send_keys('01/01/2018')
        time.sleep(2.0)
        # clica no botão para aplicar o filtro de data
        element_id = 'applyBtn'
        ec = EC.element_to_be_clickable((By.ID, element_id))
        btn = WebDriverWait(self.driver, 20).until(ec)
        btn.click()
        time.sleep(5.0)

        def calcula_fator(x):
            splitted = x.split(':')
            y = float(splitted[0]) / float(splitted[1])
            y = round(y, 3)
            return y

        # prepara html
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        element_id = 'stock-splitCalendarData'
        # obtem seleção do html
        html = soup.find_all(id=element_id)
        # obtém dataframe
        df = pd.read_html(str(html))[0]
        # corrige nome das colunas
        df.columns = ['data', 'ticker', 'fator']
        # normaliza ticker
        df['ticker'] = df['ticker'].apply(lambda x: x.split('(')[-1])
        df['ticker'] = df['ticker'].apply(lambda x: x.replace(')', ''))
        # calcula e normaliza fator
        df['fator'] = df['fator'].apply(lambda x: calcula_fator(x))
        # corrige dypes
        df = df.astype({'data': 'datetime64'})
        # corrige gaps em data
        df.fillna(method='ffill', inplace=True)
        # salva a tabela original de splits
        Database.atualizar_database(df, self.tablename_splits)

    def _cria_ou_atualiza_tabela_de_desdobramentos(self):
        # busca a tabela original no banco
        df = Database.get(f"select * from {self.tablename_splits}")
        # arreronda os valores quebrados
        df['fator'] = df['fator'].apply(lambda fator: round(fator, 0))
        # seleciona somente valores maiores que 1
        s = df['fator'] > 1
        desdobramentos = df[s].copy()
        # cria ou atualiza tabela no banco de dados
        Database.atualizar_database(desdobramentos, self.tablename_desdobramentos)

    def _cria_ou_atualiza_tabela_de_bonificacoes(self):
        # busca a tabela original no banco
        df = Database.get(f"select * from {self.tablename_splits}")
        # arreronda os valores quebrados
        df['fator'] = df['fator'].apply(lambda fator: round(fator, 0))
        # seleciona somente valores maiores que 1
        s = df['fator'] > 1
        desdobramentos = df[s].copy()
        # cria ou atualiza tabela no banco de dados
        Database.atualizar_database(desdobramentos, self.tablename_desdobramentos)

    def atualizacao(self):
        self._habilita_driver()
        self._login()
        self._busca_e_salva_tabela_de_splits()
        self._cria_ou_atualiza_tabela_de_desdobramentos()
        self.driver.quit()
