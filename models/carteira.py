# bibliotecas
import pandas as pd
# módulos externos
from decouple import config
# from datetime import datetime
# módulos internos
from models.database import Database
from models.crawlers import CrawlerCei, CrawlerInvesting


class Carteira:

    def __init__(self):

        # nome da tabelas no banco de dados
        self.tablename_negociacoes = 'negociacoes'
        self.tablename_negociacoes_ajustadas = 'negociacoes_ajustadas'
        self.tablename_desdobramentos = 'desdobramentos'
        self.tablename_ativos = 'ativos'

        """ criação de outros atributos """
        # 1: negociacoes
        try:
            # atributo com extrato original de negociações do sei
            self.negociacoes = Database.get(f"select * from {self.tablename_negociacoes}")
        except Exception:
            try:
                # cria tabela e gera atributo, se ela não existir
                self.preparacao_inicial()
            except Exception:
                self.atualizar_negociacoes()
        # 2: desmembramentos
        try:
            # atributo com relação de eventos de desdobramento
            self.desdobramentos = Database.get(f"select * from {self.tablename_desdobramentos}")
        except Exception:
            # cria tabela e gera atributo, se ela não existir
            self.atualizar_splits()
        # 3: negociacoes_ajustadas
        try:
            # atributo com negociações atualizadas com splits
            self.negociacoes_ajustadas = Database.get(f"select * from {self.tablename_negociacoes_ajustadas}")
        except Exception:
            # cria tabela e gera atributo, se ela não existir
            self._ajustar_negociacoes_com_splits(self.negociacoes)
        # 4: posicao
        self.posicao = self.calcular_posicao()
        try:
            self.ativos = Database.get(f"select * from {self.tablename_ativos}")
        except Exception:
            # cria a tabela de ativos, se não existir
            self.ativos = self._identifica_os_ativos()

    def preparacao_inicial(self, xlsx=config('PATH_CARGA_INICIAL'), sobrescrever=False):
        # realiza carga inicial
        self._dar_carga_inicial(xlsx=xlsx, sobrescrever=sobrescrever)
        # realiza atualização
        self.atualizacao()

    def atualizar_negociacoes(self):
        # atualiza negociações no extrato do CEI
        self._atualizar_negociacoes_no_cei()
        # atualiza atributo com extrato original de negociacoes
        self.negociacoes = Database.get(f"select * from {self.tablename_negociacoes}")

    def atualizar_splits(self):
        # cria objeto para realizar crawler
        i = CrawlerInvesting()
        # atualização geral dos atributos do objeto
        i.atualizacao()
        # atualiza atributo
        self.desdobramentos = Database.get(f"select * from {self.tablename_desdobramentos}")
        # aplica desdobramentos
        self._ajustar_negociacoes_com_splits(self.negociacoes)

    def atualizacao(self):
        self.atualizar_negociacoes()
        self.atualizar_splits()

    @staticmethod
    def _numerar_duplicatas(df):
        df['iguais'] = 0
        for indice, row in df.iterrows():
            selecao = df.eq(row)
            grupo_repetido = df[selecao].dropna()
            # itera no grupo
            n = 1
            for i in grupo_repetido.iterrows():
                # slice e numeração sequencial
                indice = i[0]
                df.loc[indice, 'iguais'] = n
                n = n + 1
        return df

    def _dar_carga_inicial(self, xlsx=config('PATH_CARGA_INICIAL'), sobrescrever=False):
        """ Realiza carga inicial manual de dados no banco de dados.
            A carga é feita a partir de um csv. Verifique o formato necessário. """
        # modelo de colunas do arquivo:
        """ 
        'data',
        'operacao', 
        'ticker', 
        'qtd', 
        'preco', 
        'valor_total', 
        'corretora',
        'qtd_ajustada', 
        'valor_ajustado', 
        'taxas', 
        'aquisicao_via'
        """
        # nome da tabela com as negociações originais no banco de dados
        tablename = self.tablename_negociacoes
        # safeswitch para evitar sobrescrição adicental do banco
        try:
            df = Database.get(f'select * from {tablename}')
            if len(df):
                warn = "Já existem dados na database. Caso deseje sobrescrever, atribua True ao parâmetro 'sobrescrever'"
        except Exception:
            pass
        try:
            if not sobrescrever:
                # noinspection PyUnboundLocalVariable
                return print(warn)
            else:
                pass
        except Exception:
            pass
        # importa dados em um dataframe pandas
        df = pd.read_excel(f"input/{xlsx}")
        # corrige tipo de dados
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)
        df['data'] = df['data'].dt.date
        # numera ocorrências com mesmo valor no mesmo dia para evitar serem reconhecidas como duplicatas
        df = Carteira._numerar_duplicatas(df)
        # ordena valores
        df = df.sort_values(by='data')
        # normaliza o índice para conter id's únicos
        df.reset_index(drop=True, inplace=True)
        # grava no banco de dados
        Database.atualizar_database(df, tablename)
        # atualiza atributo
        self.negociacoes = Database.get(f"select * from {self.tablename_negociacoes}")
        return df

    def _atualizar_negociacoes_no_cei(self):
        """ Incorpora o dataframe de negociações no banco de dados """
        # cria um objeto de crawler
        cei = CrawlerCei()
        # nome da tabela com as negociações originais no banco de dados
        tablename = self.tablename_negociacoes
        # lista de dataframes para concatenar
        dfs_to_concat = []
        # verifica se já há dados na tabela e resgata os dados
        try:
            conn = Database.connection()
            sql = f'SELECT * from {tablename}'
            df_atual = pd.read_sql(sql=sql, con=conn)
            dfs_to_concat.append(df_atual)
            conn.close()
        except Exception:
            pass
        # recupera dados presentes no extrato de negociações do CEI atualmente
        df_novo = cei.negociacoes_recentes()
        # normaliza o índice para conter id's únicos
        df_novo.reset_index(drop=True, inplace=True)
        # numera ocorrências com mesmo valor no mesmo dia para evitar serem reconhecidas como duplicatas
        df_novo = Carteira._numerar_duplicatas(df_novo)
        # adiciona dataframe novo à lista para concetenar
        dfs_to_concat.append(df_novo)
        # concatena os dataframes
        df = pd.concat(dfs_to_concat)
        df.reset_index(drop=True, inplace=True)
        # df.drop_duplicates(inplace=True)
        # adiciona coluna para distinguir dados dos carregados manualmente
        # selecao = df['carga_manual'] != True
        # df.loc[selecao, 'carga_manual'] = True
        # ordena valores
        df = df.sort_values(by='data')
        # grava o dataframe no banco de dados
        Database.atualizar_database(df, tablename)
        return df

    def _ajustar_negociacoes_com_splits(self, negociacoes):
        desmembramentos = self.desdobramentos

        def aplica_splits_nas_negociacoes(row):
            # seleciona somente os desdobramentos do ticker
            selecao = desmembramentos['ticker'] == row['ticker']
            desdobramentos_ticker = desmembramentos[selecao]
            # aplica efeitos dos desdobramentos, casos existam
            if len(desdobramentos_ticker):
                new_row = row.copy()
                # itera sobre cada desdobramento
                for index, linha in desdobramentos_ticker.iterrows():
                    # obtém a data do desdobramento
                    data_split = linha['data']
                    # obtém data da negociação
                    data_negociacao = row['data']
                    # obtém fator de desdobramento
                    fator_split = round(linha['fator'], 3)
                    # aplica fator se o desdobramento ocorreu após negociação
                    if (data_split > data_negociacao) and (fator_split > 0):
                        # evita a aplicação duplicada
                        new_row['qtd'] = round(new_row['qtd'] * fator_split, 0)
                        new_row['qtd_ajustada'] = round(new_row['qtd_ajustada'] * fator_split, 0)
                        new_row['preco'] = new_row['preco'] / fator_split
                        new_row['valor_total'] = new_row['valor_total'] / fator_split
                        new_row['valor_ajustado'] = new_row['valor_ajustado'] / fator_split
                        new_row['splitted'] = True
                    return new_row
            else:
                return row

        # copia para não sobrescrever
        df = negociacoes.copy()
        # acrescenta coluna para indicar aplicação de splits
        df['splitted'] = False
        # aplica splits
        df.apply(lambda row: aplica_splits_nas_negociacoes(row), axis=1)
        # grava no banco de dados
        Database.atualizar_database(df, self.tablename_negociacoes_ajustadas)
        # atualiza atributo
        self.negociacoes_ajustadas = Database.get(f"select * from {self.tablename_negociacoes_ajustadas}")

    def calcular_posicao(self):
        df = self.negociacoes_ajustadas
        # agrupa tickers para calcular posição
        posicao = df.groupby(df['ticker']).sum()['qtd_ajustada'].to_frame()
        # renomeia coluna coluna
        posicao.columns = ['posicao']
        # remove as posições zeradas
        posicao = posicao[posicao['posicao'] != 0]
        # reseta o index
        posicao.reset_index(inplace=True)
        # remove os direitos de subscrição vendidos
        subscricoes_vendidas = posicao['ticker'].str.endswith('12')
        posicao = posicao[~subscricoes_vendidas]
        return posicao

    def _identifica_os_ativos(self):
        # identifica todos os ativos
        # # passo1 : agrupa negociações por ativos
        grupo = self.negociacoes.groupby('ticker')
        # # passo2 : obtém lista somente com os tickers
        chaves = grupo.groups.keys()
        # cria um dataframe a partir da lista
        ativos = pd.DataFrame({'ticker': chaves})
        return ativos

    def _preenche_tabela_de_ativos(self):
        pass
