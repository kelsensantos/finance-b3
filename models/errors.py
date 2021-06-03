import pandas as pd

# lista para apensamento de eventuais erros e tratamentos posteriores
errors = []


def show_errors(x=errors):
    """ Cria um dataframe Pandas com o conjunto de erros gerados """
    df = pd.DataFrame(x)
    return df


def add_error(local, erro, params):
    """ Adiciona um erro na variável "errors" """
    errors.append([local, erro, params])
