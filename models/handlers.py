# bibliotecas
import pandas as pd
# módulos externos
from datetime import datetime
# módulos internos
from models.classes_objects import Etfs
from models.classes import Ativo
from models.database import Database
from models.cei import CrawlerCei


def tipo_ticker(ticker: str):
    """ Retorna um tipo de acordo com o ticker. Exemplo: 'eft', 'ação', 'fii', etc."""
    # Se listado na classe de etfs, retorna 'etf'
    if Etfs.eh_etf(ticker):
        return "etf"
    else:
        # Casos não retornem as situações anteriores, caso encontrado no Yashoo Finance returna o tipo "ação"
        ativo = Ativo(ticker)
        return ativo.tipo()


def numerar_duplicatas(df1):
    """ Numera ocorrências com mesmos valores nos mesmos dias, para evitar serem reconhecidas como duplicatas """
    # cria uma coluna para tratamento das linhas com valores iguais
    df1['iguais'] = 0
    # normaliza o índice, tornando valores únicos
    df1.reset_index(drop=True, inplace=True)
    # faz um slice com linhas que contém valores repetidos
    eh_repetido = df1.duplicated()
    repetidos = df1.loc[eh_repetido, :]
    # colhe lista com índices dessas linhas
    indices_repetidos = list(repetidos.index)
    # itera sobre as linhas
    for i in indices_repetidos:
        # lógica de seleção
        eh_igual = (df1 == df1.loc[i])
        linhas_iguais = df1[eh_igual].dropna(axis=0)
        indices_linhas_iguais = list(linhas_iguais.index)
        # slice com linhas iguais reatribuindo valor sequencial à coluna "igual"
        n = 1
        for x in indices_linhas_iguais:
            df1.loc[x, 'iguais'] = n
            n = n + 1
    return df1


def atualiza_negociacoes():
    """ Incorpora o dataframe de negociações no banco de dados """
    # cria um objeto de crawler
    cei = CrawlerCei()
    # Resgata os dados atualmente existentes no banco de dados
    tablename = 'negociacoes'
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
    df_novo = numerar_duplicatas(df_novo)
    # adiciona dataframe novo à lista para concetenar
    dfs_to_concat.append(df_novo)
    # concatena os dataframes
    df = pd.concat(dfs_to_concat)
    df.reset_index(drop=True, inplace=True)
    df.drop_duplicates(inplace=True)
    # grava o dataframe no banco de dados
    engine = Database.engine()
    df.to_sql(
        name=tablename,
        con=engine,
        if_exists='replace',
        index=False
    )
    # salva cópia de segurança
    filename = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    df.to_csv(
        f"output/security_copy_negociacoes_{filename}.csv",
        sep=';',
        index=False
    )
    engine.dispose()
    return df


def carga_inicial(xlsx='extrato_b3_negociacoes_inicial.xlsx', drop=False):
    """ Realiza carga inicial manual de dados no banco de dados.
        A carga é feita a partir de um csv. Verifique o formato necessário. """
    """ modelo de colunas do arquivo:
    'data', 'operacao', 'ticker', 'qtd', 'preco', 'valor_total', 'corretora',
    'qtd_ajustada', 'valor_ajustado', 'taxas', 'aquisicao_via'
    """
    # nome da tabela com as negociações no banco de dados
    tablename = 'negociacoes'
    # safeswitch para evitar sobrescrição adicental do banco
    try:
        df = Database.get(f'select * from {tablename}')
        if len(df):
            return print("Já existem dados na database. Caso deseje sobrescrever, atribua True ao parâmetro 'sobrescrever'"
    except Exception:
        pass
    # importa dados em um dataframe pandas
    df = pd.read_excel(f"input/{xlsx}")
    df['data'] = pd.to_datetime(df['data'], dayfirst=True)
    df['data'] = df['data'].dt.date
    # numera ocorrências com mesmo valor no mesmo dia para evitar serem reconhecidas como duplicatas
    df = numerar_duplicatas(df)
    # grava no banco de dados
    engine = Database.engine()
    df.to_sql(
        name=tablename,
        con=engine,
        if_exists='replace',
        index=False
    )
    engine.dispose()
    return df
