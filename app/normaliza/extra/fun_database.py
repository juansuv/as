# -*- coding: utf-8 -*-
import os
import math
import pandas as pd
from django.core.files.storage import FileSystemStorage
from django.core.files.base import ContentFile
import numpy as np
from . import input_struct
import cx_Oracle
import pyodbc
import datetime
from requests import Session
from zeep.transports import Transport
from zeep import Client
import tempfile
from normaliza.extra.db_parallel_operations import SqlParallel
from sqlalchemy import create_engine, event
import urllib


def generate_dir(user_id, unique_path):
    fs = FileSystemStorage(f'media/{user_id}/{unique_path}')
    if not fs.exists('Log_alistamiento.csv'):
        alistamiento = ContentFile("Iniciando log de alistamiento")
        fs.save('Log_alistamiento.log', alistamiento)
        fs.delete('Log_alistamiento.log')
    return f'media/{user_id}/{unique_path}'


def read_sql_tmpfile(query, conn):
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=query, head="HEADER"
        )
        conn
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)
        df = pd.read_csv(tmpfile)
        return df


def cargue_oracle(datos, query, num_register, params=None, unique_id="", user_id=0):
    '''
    Función para realizar una conexión a un servidor de bases de datos Oracle y extraer información
    :param db_info: variable de tipo struct_input.LoadDataBase, con la informqación de conexión respectiva.
    :param query: Query para extraer la información
    :return: Pandas DataFrame con los datos de la base de datos que cumpla la estructura dada.
    '''

    db_info = {
        'server': 'DEV614.tecnoquimicas.com',
        'sid': 'prueba',
        'usuario': 'DTMPE',
        'password': 'dtmpepr',
        'port': 1528,
        'CONSULTA': '''select  nombre_fuente,fecha_inicio,           
                           ((prom_periodo) * (vector_promedio)) AS datamult
                           from normaliza_normalizacion''',
        'COLUMNAS': ['nombre_fuente', 'fecha_inicio', 'datamult'],

    }
    db_info.update(datos)

    dsn_tns = cx_Oracle.makedsn(db_info['server'], db_info['port'], service_name=db_info['sid'])
    ora_conn = cx_Oracle.connect(user=db_info['usuario'], password=db_info['password'], dsn=dsn_tns)

    # sqlparallel.get_data(query)
    # df_ora = pd.read_sql(query, con=ora_conn, params=params)
    ora_conn.close()

    sqlparallel = SqlParallel()
    # dir_path = generate_dir(user_id,unique_id)

    result = sqlparallel.get_data_oracle(db_info, query=query, expected_data=num_register)
    return result


def guardar_oracle(datos, query, num_register, unique_id, params=None):
    '''
    Función para realizar una conexión a un servidor de bases de datos Oracle y extraer información
    :param db_info: variable de tipo struct_input.LoadDataBase, con la informqación de conexión respectiva.
    :param query: Query para extraer la información
    :return: Pandas DataFrame con los datos de la base de datos que cumpla la estructura dada.
    '''

    db_info = {
        'server': 'DEV614.tecnoquimicas.com',
        'sid': 'SWHTEST',
        'usuario': 'DTMPE',
        'password': 'dtmpepr',
        'port': 1528,
        'CONSULTA': '''select  nombre_fuente,fecha_inicio,           
                           ((prom_periodo) * (vector_promedio)) AS datamult
                           from normaliza_normalizacion''',
        'COLUMNAS': ['nombre_fuente', 'fecha_inicio', 'datamult'],

    }
    db_info.update(datos)
    db_info['unique_id'] = str(unique_id)

    sqlparallel = SqlParallel()
    # dir_path = generate_dir(user_id,unique_id)

    result = sqlparallel.save_data_oracle(db_info, query=query, expected_data=num_register)
    return result


def ejecutar_oracle(datos, query, params=None):
    '''
    Función para realizar una conexión a un servidor de bases de datos Oracle y extraer información
    :param db_info: variable de tipo struct_input.LoadDataBase, con la informqación de conexión respectiva.
    :param query: Query para extraer la información
    :return: Pandas DataFrame con los datos de la base de datos que cumpla la estructura dada.
    '''

    db_info = {
        'server': 'DEV614.tecnoquimicas.com',
        'sid': 'prueba',
        'usuario': 'DTMPE',
        'password': 'dtmpepr',
        'port': 1528,
        'CONSULTA': '''select  nombre_fuente,fecha_inicio,           
                           ((prom_periodo) * (vector_promedio)) AS datamult
                           from normaliza_normalizacion''',
        'COLUMNAS': ['nombre_fuente', 'fecha_inicio', 'datamult'],

    }
    db_info.update(datos)

    dsn_tns = cx_Oracle.makedsn(db_info['server'], db_info['port'], service_name=db_info['sid'])
    ora_conn = cx_Oracle.connect(user=db_info['usuario'], password=db_info['password'], dsn=dsn_tns)
    cur = ora_conn.cursor()
    cur.execute(query)
    res = cur.fetchall()

    ora_conn.close()
    return res


def cargue_sqlserver(datos, query, num_register, params=None):
    '''cargar archivos desde base de datos sql'''
    db_info = {
        'server': 'localhost',
        'sid': 'normaliza2',
        'usuario': 'juan',
        'password': 'estudie123',
        'CONSULTA': '''select  nombre_fuente,fecha_inicio,           
                       ((prom_periodo) * (vector_promedio)) AS datamult
                       from normaliza_normalizacion''',
        'COLUMNAS': ['nombre_fuente', 'fecha_inicio', 'datamult'],

    }
    db_info.update(datos)
    sqlparallel = SqlParallel()
    result = sqlparallel.get_data_sql(db_info, query=query, expected_data=num_register)
    # df = pd.read_sql(query, conn, params=params)
    print("reuslt,",result)

    return result


def guardar_sqlserver(datos, query, num_register, unique_id, params=None):
    '''cargar archivos desde base de datos sql'''
    db_info = {
        'server': 'localhost',
        'sid': 'normaliza2',
        'usuario': 'juan',
        'password': 'estudie123',
        'CONSULTA': '''select  nombre_fuente,fecha_inicio,           
                       ((prom_periodo) * (vector_promedio)) AS datamult
                       from normaliza_normalizacion''',
        'COLUMNAS': ['nombre_fuente', 'fecha_inicio', 'datamult'],

    }
    db_info.update(datos)

    db_info['unique_id'] = str(unique_id)
    sqlparallel = SqlParallel()
    # dir_path = generate_dir(user_id,unique_id)

    result = sqlparallel.save_data_sql(db_info, query=query, expected_data=num_register)
    return result


def ejecutar_sqlserver(datos, query, params=None):
    '''cargar archivos desde base de datos sql'''
    db_info = {
        'server': 'DESKTOP-7C966PH',
        'sid': 'normaliza2',
        'usuario': 'juan',
        'password': 'estudie123',
        'CONSULTA': '''select  nombre_fuente,fecha_inicio,           
                       ((prom_periodo) * (vector_promedio)) AS datamult
                       from normaliza_normalizacion''',
        'COLUMNAS': ['nombre_fuente', 'fecha_inicio', 'datamult'],

    }
    db_info.update(datos)
    print("datos con los que se conecta sql server",db_info)

    '''quoted = urllib.parse.quote_plus(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + db_info['server'] + ';DATABASE=' + db_info['sid'] + '; UID = ' +
        db_info['usuario'] + '; PWD = ' + db_info['password'] + ';Trusted_Connection=no;')

    print("quute", quoted)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    con = engine.connect()
    result = con.execute(query)
    for row in result:
        print(row)
    '''

    string_connect2=('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + db_info['server'] + ';DATABASE=' + db_info['sid'] + ';UID=' + db_info['usuario'] + ';PWD=' + db_info['password'])
    print("conecta con los datos",string_connect2)
    conn = pyodbc.connect(string_connect2)
    # conn2 = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Userstr\suarez\Desktop\testdb.accdb;')
    cur = conn.cursor()
    print(query)
    cur.execute(query)


    res=0
    try:
        res=cur.fetchall()
        print()
    except:
        pass
    conn.close()
    return res


def homologar_estructura(dfQuery, meta):
    '''
    Se recibe el Dataframe generado por el query y el meta para renombrar y homologar columnas
    :param dfQuery: Dataframe a renombrar y homologar
    :param meta: Estructura de homologacion de formato LoadFile
    :return: Dataframe homologado
    '''
    pm_file = dfQuery[meta.columns]
    pm_file.rename(columns=meta.columns_rename, inplace=True)
    return pm_file


def subir_info_error(db_info, log):
    '''
    Funcion para hacer un insert en la tabla de errores de PYG Audiencia
    :param db_info: Información de conexión a la base de datos
    :param log: Paramtros a insertar en forma de lista
    :return:
    '''
    dsn_tns = cx_Oracle.makedsn(db_info.server, db_info.port, service_name=db_info.sid)
    ora_conn = cx_Oracle.connect(user=db_info.user, password=db_info.password, dsn=dsn_tns, encoding='UTF-8',
                                 nencoding='UTF-8')
    c = ora_conn.cursor()
    query = "insert into pyg_errores (fecha, eds, usuario, error_tecnico, desc_error_tecnico, desc_error_usuario, tipo_error, nivel_error, proceso, archivo_log ) values (to_date(:fecha,'YYYY-MM-DD HH24:MI:SS'),:eds,:usuario,:error_tecnico,:desc_error_tecnico,:desc_error_usuario,:tipo_error,:nivel_error,:proceso,:archivo_log)"
    c.execute(query, log)
    ora_conn.commit()
    ora_conn.close()


def tiempo_espera_web_service(df_info, query):
    '''
    Funcion para hacer un query a una base
    :param db_info: Información de conexión a la base de datos
    :param query: query a ejectuar
    :return:
    '''
    dsn_tns = cx_Oracle.makedsn(df_info.server, df_info.port, service_name=df_info.sid)
    ora_conn = cx_Oracle.connect(user=df_info.user, password=df_info.password, dsn=dsn_tns, encoding='UTF-8',
                                 nencoding='UTF-8')
    c = ora_conn.cursor()
    c.execute(query)
    var_return = c.fetchone()
    ora_conn.close()
    return (var_return)


def generar_clinte_web_service(wsdl, time_out):
    '''
    Se genera un cliente de conexión al web service que entra por parametro
    :param wsdl: WSDL del web service
    :param time_out: Tiempo global de time-out para las operaciones del web service
    :return: Cliente de web service
    '''
    try:
        session = Session()
        session.verify = False
        transport = Transport(session=session, operation_timeout=time_out)
        client = Client(wsdl=wsdl, transport=transport)
        return client
    except:
        return False
