from os import walk, getcwd, path
import math
import pandas as pd
import numpy as np
from . import input_struct
import re


def ls(regex='', ruta=getcwd()):
    pat = re.compile(regex, re.I)
    resultado = []
    for (dir, _, archivos) in walk(ruta):
        resultado.extend([path.join(dir, arch) for arch in
                          filter(pat.search, archivos)])
        # break  # habilitar si no se busca en subdirectorios
    return resultado


def cargue_csv(file_address, meta, sep=',', encoding='utf-8', low_memory=True):
    '''
    Esta función sirve para realizar el cargue de archivos csv al sistema para procesarlos.
    :param file_address: Direccion donde se encuentra el archivo a cargar
    :param meta: Estructura de tipo LoadFile (definida en struct_input.py del archivo a cargar)
    :param sep: Separador del archivo csv ( es decir ',' , '|', entre otros)
    :param encoding: Tipo de codificacion con la que va a ser cargado el archivo
    :param low_memory: booleano que indica si hacer uso de las propiedades de optimizacion de memoria de pandas.
                       Por defecto, se tiene en True para cargues rapidos, para archivos muy pesados debe ser False
    :return: Pandas DataFrame con los datos del archivo validado que cumpla la estructura dada.
    '''
    pm_file = pd.read_csv(
        file_address,
        sep=sep,
        encoding=encoding,
        dtype=meta.columns_dtypes,
        low_memory=low_memory
    )
    # Quita caracteres en blanco antes y despues de los titulos de columnas
    names = {x: str(x).strip() for x in pm_file.columns}
    pm_file.rename(names, axis=1, inplace=True)
    pm_file = pm_file[meta.columns]
    pm_file.rename(columns=meta.columns_rename, inplace=True)

    return pm_file


def cargue_xlsx(file_address, meta, sheet_name=0, header=0):
    '''
    Esta función sirve para realizar el cargue de archivos de Excel (xlsx) al sistema para procesarlos.
    :param file_address: Direccion de donde se va a cargar el archivo
    :param meta: Estructura de tipo LoadFile (definida en struct_input.py del archivo a cargar)
    :param sheet_name: Nombre de la hoja de excel a cargar en caso de que la información no esté en la primer hoja
    :return: Pandas DataFrame con los datos del archivo validado que cumpla la estructura dada.
    '''
    pm_file = pd.read_excel(
        file_address,
        sheet_name,
        header

    )
    # Quita caracteres en blanco antes y despues de los titulos de columnas
    names = {x: str(x).strip() for x in pm_file.columns}
    pm_file.rename(names, axis=1, inplace=True)
    pm_file = pm_file[meta.columns]
    pm_file.rename(columns=meta.columns_rename, inplace=True)

    return pm_file


def cargue_excel_melt(file_address, meta, var_name, value_name, header=0, sheet_name=0):
    '''
    Esta función sirve para realizar el cargue de archivos de Excel (xlsx) al sistema para procesarlos.
    Hace un pivot columns en las columnas que no hagan parte de Meta, haciendo que las columnas se vuelvan
    valores de la columna 'Var_name' con valor respectivo en la columna 'value_name'.
    :param file_address: Direccion del archivo a cargar
    :param meta: Estructura de tipo LoadFile (definida en struct_input.py del archivo a cargar)
    :param var_name: Nombre de la columna con los labes del pivot
    :param value_name: Nombre de la columna con los valores del pivot
    :param header: valores cabecera en caso de que el archivo no los tenga definidos.
    :param sheet_name: Nombre de la hoja de excel a cargar en caso de que la información no esté en la primer hoja
    :return: Pandas DataFrame con los datos del archivo validado que cumpla la estructura dada.
    '''
    pm_file = pd.read_excel(
        file_address,
        sheet_name=sheet_name,
        header=header
    )
    # Quita caracteres en blanco antes y despues de los titulos de columnas
    names = {x: str(x).strip() for x in pm_file.columns}
    pm_file.rename(names, axis=1, inplace=True)
    melt_dataframe = pd.melt(
        pm_file,
        id_vars=meta.columns,
        var_name=var_name,
        value_name=value_name
    )

    melt_dataframe = melt_dataframe[meta.columns + [var_name, value_name]]
    melt_dataframe.rename(columns=meta.columns_rename, inplace=True)

    return melt_dataframe
