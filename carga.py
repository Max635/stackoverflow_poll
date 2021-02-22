""" ETL carga.py """

import csv
import os
import sys
from io import StringIO

import numpy as np
import pandas as pd
import psycopg2

from psycopg2 import extensions, sql


DB_NAME = 'stackoverflow_poll'


def _extract_data_from_csv():
    current_dir = os.getcwd()
    data_csv_file = 'base_de_respostas_10k_amostra.csv'
    return pd.read_csv(f'{current_dir}/{data_csv_file}', encoding='utf-8')


def _transform_drop_nan_country(df):
    """ Drop Nan rows in Country Series. """
    df.dropna(subset=['Country'], inplace=True)
    return df


def _transform_select_columns(df):
    """ Select columns to work with. """
    df = df[['Respondent', 'Hobby', 'OpenSource', 'Country', 'CompanySize',\
                'ConvertedSalary', 'CommunicationTools',\
                'OperatingSystem', 'LanguageWorkedWith']]
    return df


def _transforms_replace_fillna(df):
    """ Replace and fill NaN values in series specified in column_name_list. """
    column_name_list = ['LanguageWorkedWith', 'CommunicationTools',\
        'LanguageWorkedWith', 'OperatingSystem', 'CompanySize']
    for col in column_name_list:
        df[col].replace('', np.nan, inplace=True)
        df[col].fillna('', inplace=True)
    return df


def _transforms_replace_values(df):
    """ Replace values in series specified in column_name_list. """
    column_name_list = ['Hobby', 'OpenSource']
    for col in column_name_list:
        df[col].replace('No', '0', inplace=True)
        df[col].replace('Yes', '1', inplace=True)
    return df


def _transform_fillna_converted_salary(df):
    """ Fill Nan values with 0.0 in ConvertedSalary series. """
    df['ConvertedSalary'].fillna('0.0', inplace=True)
    return df


def _transforms_type(df):
    """ Replace data types series. """
    df = df.astype(
        {
            'Hobby': 'int',
            'OpenSource': 'int',
            'CompanySize': 'str',
            'ConvertedSalary': 'float'
        })
    return df


def _calc_monthly_salary(df):
    """ Calculate monthly salary based on ConvertedSalary serie. """
    MONTH = 12
    COTACAO = 5.6
    df['salario'] = (df['ConvertedSalary'] / MONTH) * COTACAO
    df['salario'] = df['salario'].round(2)
    return df


def _drop_columns(df):
    """ Drop series from Dataframe df. """
    df.drop(['ConvertedSalary', 'Respondent'], axis=1, inplace=True)
    return df


def _build_string_respondente(df):
    """ This function creates the encoding for the 'respondente_#' string. """
    df['nome'] = [f'respondente_{x}' for x in range(1, len(df)+1)]
    return df


def _rename_columns(df):
    df.rename(
        columns={
            'OpenSource': 'contrib_open_source',
            'Hobby': 'programa_hobby',
            'OperatingSystem': 'sistema_operacional',
            'Country': 'pais',
            'CompanySize': 'tamanho',
            'CommunicationTools': 'ferramenta_comunic',
            'LanguageWorkedWith': 'linguagem_programacao'
        },
        inplace=True)
    return df


def _build_df_linguagem_programacao(df):
    new_row_list = []
    for item_value in df['linguagem_programacao'].iteritems():
        if item_value[1]:
            list_splitted = item_value[1].split(';')

            for lang in list_splitted:
                new_row = {
                    'nome': df['nome'][item_value[0]],
                    'linguagem_programacao': lang if item_value[1] else None
                }
                new_row_list.append(new_row)

    dataframe = pd.DataFrame()
    dataframe = dataframe.append(new_row_list, ignore_index=True)
    return dataframe


def _build_df_ferramenta_comunic(df):
    new_row_list = []
    for item_value in df['ferramenta_comunic'].iteritems():
        if item_value[1]:
            list_splitted = item_value[1].split(';')

            for ferramenta in list_splitted:
                new_row = {
                    'nome': df['nome'][item_value[0]],
                    'ferramenta_comunic': ferramenta if item_value[1] else None,
                }
                new_row_list.append(new_row)

    dataframe = pd.DataFrame()
    dataframe = dataframe.append(new_row_list, ignore_index=True)
    return dataframe


def _get_unique_from_multiples_values(dataframe, column_name):
    df = dataframe.drop_duplicates(subset=column_name, keep=False)
    items_list = []

    for _c in df.iteritems():
        row_value = df[column_name].values
        row_list = row_value.tolist()

        for i in row_list:
            list_splitted = i.split(';')
            for item_name in list_splitted:
                if item_name not in items_list:
                    items_list.append(item_name)
    df = pd.DataFrame(items_list, columns=[column_name])
    return df


def _connect():
    PARAMS_DB = {
        'host': '127.0.0.1',
        'user': 'postgres',
        'password': 'admin'
    }
    conn = None
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**PARAMS_DB)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


def _create_database(conn, DB_NAME):
    """ Create database in PostgreSQL """
    autocommit = extensions.ISOLATION_LEVEL_AUTOCOMMIT
    conn.set_isolation_level(autocommit)
    cursor = conn.cursor()
    cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
    cursor.close()
    conn.close()


def _create_tables():
    """ Create tables in the PostgreSQL database DB_NAME """
    commands = (
        """
        CREATE TABLE empresa (
            id SERIAL PRIMARY KEY,
            tamanho VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE ferramenta_comunic (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE linguagem_programacao (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE pais (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE sistema_operacional (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE respondente (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(255) NOT NULL,
            contrib_open_source INTEGER,
            programa_hobby INTEGER,
            salario NUMERIC,
            sistema_operacional_id INTEGER NOT NULL,
            pais_id INTEGER NOT NULL,
            empresa_id INTEGER NOT NULL,
            FOREIGN KEY (sistema_operacional_id)
                REFERENCES sistema_operacional (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (pais_id)
                REFERENCES pais (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (empresa_id)
                REFERENCES empresa (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE resp_usa_ferramenta (
            ferramenta_comunic_id INTEGER,
            respondente_id INTEGER,
            FOREIGN KEY (ferramenta_comunic_id)
                REFERENCES ferramenta_comunic (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (respondente_id)
                REFERENCES respondente (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE resp_usa_linguagem (
            respondente_id INTEGER,
            linguagem_programacao_id INTEGER,
            momento INTEGER,
            FOREIGN KEY (respondente_id)
                REFERENCES respondente (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (linguagem_programacao_id)
                REFERENCES linguagem_programacao (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """
    )
    conn = None
    conn = psycopg2.connect(host='localhost', dbname=DB_NAME,
                            user='postgres', password='admin')
    # cursor = conn.cursor()
    try:
        # params = config()
        # conn = psycopg2.connect(**params)
        cur = conn.cursor()

        for command in commands:
            cur.execute(command)

        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()


def _admin_insert_unique_values(df):
    """ This function iterate over the column_name_dict dictionary to get the columns names.
        Then, through _get_unique_from_multiples_values() function, obtains unique values
        that will be insert in the tables by using _insert_with_copy() function. """

    column_name_dict = {
        'OperatingSystem': 'sistema_operacional',
        'Country': 'pais',
        'CompanySize': 'empresa',
        'LanguageWorkedWith': 'linguagem_programacao',
        'CommunicationTools': 'ferramenta_comunic'
    }
    for item, value in column_name_dict.items():
        if item == 'LanguageWorkedWith' or item == 'CommunicationTools':
            unique_values = _get_unique_from_multiples_values(df, item)
            _insert_with_copy(unique_values, value)
        else:
            lista = df[item].unique()
            unique_values = pd.Series(lista)
            _insert_with_copy(unique_values, value)


def _insert_with_copy(df, table):
    """ Insert data in the PostgreSQL database using StringIO and COPY sql. """

    buffer = StringIO()
    df.to_csv(
        buffer,
        index=False,
        header=False,
        quoting=csv.QUOTE_NONNUMERIC,
        sep=','
        )
    buffer.seek(0)
    conn = psycopg2.connect(host='localhost', dbname=DB_NAME,
                            user='postgres', password='admin')
    cursor = conn.cursor()

    try:
        column_name = 'nome' if table != 'empresa' else 'tamanho'
        copy_sql = """ COPY {}({}) FROM STDIN WITH DELIMITER AS ',' CSV QUOTE '\"' """\
                    .format(table, column_name)
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print(f'sql_copy {table} done')
    cursor.close()
    return True


def _insert_table_respondente(df):
    print(_insert_table_respondente)
    conn = psycopg2.connect(host='localhost', dbname=DB_NAME,
                            user='postgres', password='admin')
    cursor = conn.cursor()

    for row in df.itertuples(index=False):
        insert_query = \
        """ INSERT INTO respondente(
                nome, contrib_open_source, programa_hobby,
                salario, sistema_operacional_id, pais_id, empresa_id)
            VALUES
                (%s,%s,%s,%s,
                (SELECT sistema_operacional.id FROM sistema_operacional
                    WHERE sistema_operacional.nome=%s),
                (SELECT pais.id FROM pais
                    WHERE pais.nome=%s),
                (SELECT empresa.id FROM empresa
                    WHERE empresa.tamanho=%s))"""

        record_to_insert = (
            row.nome, row.contrib_open_source,
            row.programa_hobby, row.salario,
            row.sistema_operacional, row.pais, row.tamanho
        )

        cursor.execute(insert_query, record_to_insert)
        conn.commit()
    cursor.close()
    conn.close()


def _insert_table_resp_usa_ferramenta(df):
    print(_insert_table_resp_usa_ferramenta)
    conn = psycopg2.connect(host='localhost', dbname=DB_NAME,
                            user='postgres', password='admin')
    cursor = conn.cursor()

    for row in df.itertuples(index=False):
        insert_query = \
        """ INSERT INTO resp_usa_ferramenta(
                ferramenta_comunic_id, respondente_id)
            VALUES (
                (SELECT ferramenta_comunic.id FROM ferramenta_comunic
                    WHERE ferramenta_comunic.nome=%s),
                (SELECT respondente.id FROM respondente
                    WHERE respondente.nome=%s)) """

        record_to_insert = (row.ferramenta_comunic, row.nome)
        cursor.execute(insert_query, record_to_insert)
        conn.commit()
    cursor.close()
    conn.close()


def _insert_table_resp_usa_linguagem(df):
    print(_insert_table_resp_usa_linguagem)
    conn = psycopg2.connect(host='localhost', dbname=DB_NAME,
                            user='postgres', password='admin')
    cursor = conn.cursor()

    for row in df.itertuples(index=False):
        insert_query = """ INSERT INTO resp_usa_linguagem(respondente_id, linguagem_programacao_id)\
             VALUES ( \
                 (SELECT respondente.id FROM respondente 
                    WHERE respondente.nome=%s), \
                 (SELECT linguagem_programacao.id FROM linguagem_programacao 
                    WHERE linguagem_programacao.nome=%s)) """
        record_to_insert = (row.nome, row.linguagem_programacao)

        cursor.execute(insert_query, record_to_insert)
        conn.commit()
    cursor.close()
    conn.close()


def main():
    """ Entrypoint function """

    dataframe = _extract_data_from_csv()
    dataframe = _transform_drop_nan_country(dataframe)
    dataframe = _transform_select_columns(dataframe)

    dataframe = _transforms_replace_fillna(dataframe)
    dataframe = _transforms_replace_values(dataframe)
    dataframe = _transform_fillna_converted_salary(dataframe)
    dataframe = _transforms_type(dataframe)

    conn = _connect()
    _create_database(conn, DB_NAME)
    _create_tables()
    _admin_insert_unique_values(dataframe)

    dataframe = _calc_monthly_salary(dataframe)
    dataframe = _build_string_respondente(dataframe)
    dataframe = _rename_columns(dataframe)
    dataframe = _drop_columns(dataframe)

    df = dataframe[['nome', 'contrib_open_source', 'programa_hobby',
                    'salario', 'sistema_operacional', 'pais', 'tamanho']]
    _insert_table_respondente(df)

    df = dataframe[['nome', 'linguagem_programacao']]
    df_linguagem_programacao = _build_df_linguagem_programacao(df)
    _insert_table_resp_usa_linguagem(df_linguagem_programacao)

    df = dataframe[['nome', 'ferramenta_comunic']]
    df_ferramenta_comunic = _build_df_ferramenta_comunic(df)
    _insert_table_resp_usa_ferramenta(df_ferramenta_comunic)


if __name__ == "__main__":
    main()
