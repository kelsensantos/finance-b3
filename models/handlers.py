# bibliotecas
import pandas as pd
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
    # se existerem dados na DB atualmente, aplica lógica para afastar duplicatas dos acrescidos
    try:
        ja_incluidos = df_novo.isin(df_atual)
        df_ja_incluidos = df_novo[ja_incluidos].dropna(how='all', axis=0)
        indices_ja_incluidos = list(df_ja_incluidos.index)
        df_novo.drop(indices_ja_incluidos, inplace=True)
    except Exception:
        pass
    dfs_to_concat.append(df_novo)
    # concatena os dataframes
    df = pd.concat(dfs_to_concat)
    # grava o dataframe no banco de dados
    engine = Database.engine()
    df.to_sql(
        name=tablename,
        con=engine,
        if_exists='replace',
        index=False
    )
    engine.dispose()
    return df


def carga_inicial(csv):
    """ Realiza carga inicial manual de dados no banco de dados.
        A carga é feita a partir de um csv. Verifique o formato necessário. """
    # modelo de colunas do arquivo:
    # 'data', 'operacao', 'ticker', 'qtd', 'preco', 'valor_total', 'corretora',
    # 'qtd_ajustada', 'valor_ajustado', 'taxas', 'aquisicao_via'

