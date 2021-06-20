# bibliotecas
import pandas as pd
# módulos externos
from decouple import config
# from datetime import datetime
# módulos internos
from models.database import Database
from models.crawlers import CrawlerCei, CrawlerInvesting, CrawlerAdvfn, Handlers, CrawlerBvfm


class Carteira:

    def __init__(self):

        # nome da tabelas no banco de dados
        self._tablename_negociacoes = 'negociacoes'
        self._tablename_negociacoes_ajustadas = 'negociacoes_ajustadas'
        self._tablename_desdobramentos = 'desdobramentos'
        self._tablename_ativos = 'ativos'
        # criação dos atributos atributos principais
        self.negociacoes = self._set_negociacoes()
        self.desdobramentos = self._set_desdobramentos()
        self.negociacoes_ajustadas = self._set_negociacoes_ajustadas()
        self.posicao = self._set_posicao()

    def _set_negociacoes(self):
        try:
            # atributo com extrato original de negociações do sei
            df = Database.get(f"select * from {self._tablename_negociacoes}")
            return df
        except Exception:
            # cria tabela e gera atributo, se ela não existir
            self.preparacao_inicial()
            return Database.get(f"select * from {self._tablename_negociacoes}")

    def _set_desdobramentos(self):
        try:
            # atributo com relação de eventos de desdobramento
            df = Database.get(f"select * from {self._tablename_desdobramentos}")
            return df
        except Exception:
            # cria tabela e gera atributo, se ela não existir
            self.atualizar_splits()
            return Database.get(f"select * from {self._tablename_desdobramentos}")

    def _set_negociacoes_ajustadas(self):
        try:
            # atributo com negociações atualizadas com splits
            df = Database.get(f"select * from {self._tablename_negociacoes_ajustadas}")
            return df
        except Exception:
            # cria tabela e gera atributo, se ela não existir
            self._ajustar_negociacoes_com_splits(self.negociacoes)
            return Database.get(f"select * from {self._tablename_negociacoes_ajustadas}")

    def _set_posicao(self):
        posicao = self._calcular_posicao()
        return posicao

    def preparacao_inicial(self, xlsx=config('PATH_CARGA_INICIAL'), sobrescrever=False):
        try:
            # realiza carga inicial
            self._dar_carga_inicial(xlsx=xlsx, sobrescrever=sobrescrever)
        except Exception:
            pass
        # realiza atualização
        self.atualizacao()

    def atualizar_negociacoes(self):
        # atualiza negociações no extrato do CEI
        self._atualizar_negociacoes_no_cei()
        # atualiza atributo com extrato original de negociacoes
        self.negociacoes = Database.get(f"select * from {self._tablename_negociacoes}")

    def atualizar_splits(self):
        # cria objeto para realizar crawler
        i = CrawlerInvesting()
        # atualização geral dos atributos do objeto
        i.atualizacao()
        # atualiza atributo
        self.desdobramentos = Database.get(f"select * from {self._tablename_desdobramentos}")
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
        tablename = self._tablename_negociacoes
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
        self.negociacoes = Database.get(f"select * from {self._tablename_negociacoes}")
        return df

    def _atualizar_negociacoes_no_cei(self):
        """ Incorpora o dataframe de negociações no banco de dados """
        # cria um objeto de crawler
        cei = CrawlerCei()
        # nome da tabela com as negociações originais no banco de dados
        tablename = self._tablename_negociacoes
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

        def _aplica_splits_nas_negociacoes(row, desdobramentos):
            # seleciona somente os desdobramentos do ticker
            selecao = desdobramentos['ticker'] == row['ticker']
            desdobramentos_ticker = desdobramentos[selecao]
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
        df.apply(lambda row: _aplica_splits_nas_negociacoes(row, desdobramentos=self.desdobramentos), axis=1)
        # grava no banco de dados
        Database.atualizar_database(df, self._tablename_negociacoes_ajustadas)
        # atualiza atributo
        self.negociacoes_ajustadas = Database.get(f"select * from {self._tablename_negociacoes_ajustadas}")

    def _calcular_posicao(self):
        df = self.negociacoes_ajustadas
        # agrupa tickers para cálculo da posição
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

    """ métodos sobre os dados dos ativos """

    def _identifica_os_ativos_negociados(self):
        """ Identifica todos os ativos na certeira. """
        # passo1 : agrupa negociações por ativos
        grupo = self.negociacoes.groupby('ticker')
        # passo2 : obtém lista somente com os tickers
        chaves = grupo.groups.keys()
        # passo3 : cria um dataframe a partir da lista
        ativos_atualizados = pd.DataFrame({'ticker': chaves})
        # retorna a lista de ativos
        return ativos_atualizados

    def atualizar_ativos(self):
        df = self._identifica_os_ativos_negociados()
        df.apply(lambda row: Ativo(row['ticker']), axis=1)

    def buscar_ativos(self):
        try:
            df = Database.get(f'select * from {self._tablename_ativos}')
        except Exception:
            self.atualizar_ativos()
            df = Database.get(f'select * from {self._tablename_ativos}')
        return df


class Ativo:

    def __init__(self, ticker):
        # crawlers, etc
        self.advfn = CrawlerAdvfn(ticker=ticker)
        # tabelas no banco de dados
        self._tablename_ativos = 'ativos'
        self._tablename_empresa = 'ativos_empresa'
        self._tablename_eventos_corporativos = 'eventos_corporativos'
        self._tablename_rendimentos = 'rendimentos'
        self._tablename_rendimentos_acoes = 'rendimentos_acoes'
        # verifica regularidade do banco
        self._verifica_regularidade_do_banco()
        # atributos principais sobre o ativo
        self.ticker = self._set_ticker(ticker)
        self.codigo_isin = self._set_codigo_isin()
        self.tipo = self._set_tipo()
        self.empresa = self._set_empresa()
        self.rendimentos = self._set_rendimentos()
        self.eventos_corporativos = self._set_eventos_coporativos()

    def _verifica_regularidade_do_banco(self):
        if not Database.verificar_existencia_de_tabela(self._tablename_ativos):
            Database.execute(
                f""" 
                    CREATE TABLE {self._tablename_ativos} (
                        ticker VARCHAR NOT NULL,
                        codigo_isin VARCHAR,
                        tipo VARCHAR,
                        PRIMARY KEY(ticker) 
                    )
                """
            )
        if not Database.verificar_existencia_de_tabela(self._tablename_eventos_corporativos):
            Database.execute(
                f""" 
                    CREATE TABLE {self._tablename_eventos_corporativos} (
                        evento VARCHAR,
                        codigo_isin VARCHAR,
                        data_deliberacao DATE,
                        negocios_ate DATE,
                        fator FLOAT8,
                        ativo_emitido VARCHAR,
                        observacoes TEXT
                    )
                """
            )
        if not Database.verificar_existencia_de_tabela(self._tablename_rendimentos):
            Database.execute(
                f""" 
                    CREATE TABLE {self._tablename_rendimentos} (
                        tipo_rendimento VARCHAR,
                        codigo_isin VARCHAR,
                        data_deliberacao DATE,
                        negocios_ate DATE,
                        valor FLOAT8,
                        relativo_a VARCHAR,
                        ativo_emitido VARCHAR,
                        data_pagamento DATE,
                        observacoes TEXT
                    )
                """
            )

    def _set_ticker(self, ticker):
        df = Database.get(f"select ticker from {self._tablename_ativos} where ticker = '{ticker}'")
        if len(df):
            return ticker
        else:
            Database.execute(f"INSERT INTO {self._tablename_ativos}(ticker) values('{ticker}')")
            return ticker

    def _set_codigo_isin(self):
        # procura no banco de dados
        df = Database.get(f"select codigo_isin from {self._tablename_ativos} where ticker = '{self.ticker}'")
        if df.loc[0][0] is not None:
            codigo_isin = df.loc[0][0]
            return codigo_isin
        else:
            codigo_isin = self.advfn.codigo_isin()
            Database.execute(
                f"UPDATE {self._tablename_ativos} SET codigo_isin = '{codigo_isin}' where ticker = '{self.ticker}'"
            )
            return codigo_isin

    def _set_tipo(self):
        # procura no banco de dados
        df = Database.get(f"select tipo from {self._tablename_ativos} where ticker = '{self.ticker}'")
        if df.loc[0][0] is not None:
            tipo = df.loc[0][0]
            return tipo
        else:
            # busca o tipo
            tipo = Handlers.tipo_ticker(self.ticker, self.codigo_isin)
            # salva no banco de dados
            Database.execute(
                f"UPDATE {self._tablename_ativos} SET tipo = '{tipo}' where ticker = '{self.ticker}'"
            )
            return tipo

    def _set_empresa(self):
        if self.tipo == 'ação':
            df = self._set_empresa_acao()
            return df
        else:
            df = pd.DataFrame()
            return df

    def _set_empresa_acao(self):
        # tenta buscar no banco de dados
        try:
            # procura no banco de dados
            df = Database.get(f"select * from {self._tablename_empresa} as t where t.codigo = '{self.ticker}'")
            if len(df):
                return df
            else:
                # procura via crawler no advfn
                df = self.advfn.dados_gerais_da_empresa()
                # faz insert na base de dados
                Database.insert_df(df, self._tablename_empresa)
                return df
        # cria a tabela no banco de dados, se ela não existir
        except Exception:
            # procura via crawler no advfn
            df = self.advfn.dados_gerais_da_empresa()
            # cria a tabela no banco de dados
            Database.atualizar_database(df, self._tablename_empresa)
            return df

    def _set_rendimentos(self):
        df = Database.get(
            f"select * from {self._tablename_rendimentos} as t where t.codigo_isin = '{self.codigo_isin}'"
        )
        return df

    def _set_eventos_coporativos(self):
        df = Database.get(
            f"select * from {self._tablename_eventos_corporativos} as t where t.codigo_isin = '{self.codigo_isin}'"
        )
        return df

    def atualiza_eventos_e_rendimentos_de_acoes(self):
        codigo_cvm = self.empresa['Código CVM'].loc[0]
        [_eventos_corporativos, _rendimentos] = CrawlerBvfm.set_tabelas_de_acoes(codigo_cvm)

        # atualiza eventos corporativos no banco
        eventos_atuais = Database.get_table(self._tablename_eventos_corporativos)
        to_concat = [eventos_atuais, _eventos_corporativos]
        eventos_atualizados = pd.concat(to_concat)
        eventos_atualizados.drop_duplicates(inplace=True)
        Database.atualizar_database(eventos_atualizados, self._tablename_eventos_corporativos)

        # atualiza rendimentos no banco
        rendimentos_atuais = Database.get_table(self._tablename_rendimentos)
        to_concat = [rendimentos_atuais, _rendimentos]
        rendimentos_atualizados = pd.concat(to_concat)
        rendimentos_atualizados.drop_duplicates(inplace=True)
        Database.atualizar_database(rendimentos_atualizados, self._tablename_rendimentos)

        # atualiza tabela com todos os rendimentos
        try:
            tabela_atual = Database.get_table(self._tablename_rendimentos)
            # remove os valores anteriores da mesma empresa/ação
            to_remove = tabela_atual['codigo'] == self.ticker[:-1]
            to_remove = list(tabela_atual[to_remove].index)
            tabela_atual.drop(to_remove)
        except Exception:
            tabela_atual = pd.DataFrame()
        tabela_atualizada = CrawlerBvfm.todos_os_rendimentos_da_acao(codigo_cvm, self.ticker)
        to_concat = [tabela_atual, tabela_atualizada]
        nova_tabela = pd.concat(to_concat)
        Database.atualizar_database(nova_tabela, self._tablename_rendimentos_acoes)

        # atualiza atributos
        self.rendimentos = self._set_rendimentos()
        self.eventos_corporativos = self._set_eventos_coporativos()

    def atualiza_eventos_e_rendimentos(self):
        if self.tipo == 'ação': self.atualiza_eventos_e_rendimentos_de_acoes()
