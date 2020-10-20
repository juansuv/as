import pandas as pd
import datetime
import sys
import glob
import uuid

from normaliza.extra.fun_database import cargue_oracle, cargue_sqlserver, guardar_oracle, guardar_sqlserver, \
    ejecutar_sqlserver, ejecutar_oracle
from normaliza.extra.fun_load import cargue_csv, cargue_xlsx, ls
from normaliza.extra.input_struct import LoadFile

import normaliza.extra.api_filters as apifil
from normaliza.extra.normalize import Normalize
from normaliza.extra.validator import validate_data
from normaliza.extra import utils as logs_normalizacion


class DataFlow:
    df_data = pd.DataFrame()
    valor = ''
    tiempo = ''
    ruta = ''
    db_info = {}
    nombre_tabla = ''
    num_register = 0
    tipo_fuente = ''
    nombre_destino = ''

    def __init__(self):
        df_data = pd.DataFrame()
        valor = ''
        tiempo = ''
        ruta = ''
        db_info = {}
        nombre_tabla = ''
        num_register = 0
        tipo_fuente = ''
        nombre_destino = ''

    def datasource_conection(self, data, user_id):
        unique_id = data['unique_id']
        fuente = data['api_paramNorm'].Fuente
        self.nombre_tabla = data['api_paramNorm'].nombre_fuente
        self.nombre_destino = data['api_paramNorm'].nombre_destino
        print(data['api_paramNorm'].parametrosbd)
        parambd2 = data['api_paramNorm'].parametrosbd.all()
        print(parambd2)
        for param in parambd2:
            self.db_info = {
                'server': param.host,
                'port': param.puerto,
                'sid': param.SID,
                'ruta': param.ruta,
                'usuario': param.usuario,
                'password': param.password,
            }
        col = []
        columns = data['columnas'].copy()
        data['ruta'] = self.db_info['ruta']
        ren = {}
        dtype = {}

        for column in columns:
            col.append(column['nombre_columna'])
            ren[column['nombre_columna']] = column['mapeo']
            if column['tipo'] == 1:
                dtype[column['nombre_columna']] = int
            elif column['tipo'] == 2:
                dtype[column['nombre_columna']] = object
            elif column['tipo'] == 3:
                dtype[column['nombre_columna']] = bool
            elif column['tipo'] == 4:
                dtype[column['nombre_columna']] = float

        load_data_struc = LoadFile(col, ren, dtype)
        self.tipo_fuente = fuente.nombre
        if fuente.nombre == 1:
            # SQL SERVER

            # se obtiene query

            final_query = apifil.get_filter_query(columns, self.nombre_tabla, fuente.nombre) + ''
            where_query = final_query.split('FROM')[1]
            print("query",final_query)
            where_query_f = f"SELECT COUNT(*) FROM" + where_query

            try:
                self.num_register = ejecutar_sqlserver(self.db_info, where_query_f, params=None)
                print("numregister",self.num_register)
            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'El Query para la Consulta en la Base de datos ORACLE con la información del cliente no cumple con los estándares establecidos.El archivo de Excel no cumple con la estructura/formato estándar definido.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Obtencion numero registros |Cargue de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_clean(error)
                raise Exception("no se obtuvieron registros")

            if int(self.num_register[0][0]) == 0:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'El filtro aplicado no obtiene datos',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Obtencion datos, no se encuentran registros|Cargue de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_clean(error)
                raise Exception('Error el filtro aplicado no obtiene registros entre los datos')

            try:
                self.df_data = cargue_sqlserver(self.db_info, final_query, self.num_register[0][0], params=None)
            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'El Query para la Consulta en la Base de datos SQL SERVER con la información del cliente no cumple con los estándares establecidos.El archivo de Excel no cumple con la estructura/formato estándar definido.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Cargue de datos',
                    'archivo_log': 'NO'

                }

                data_validation = False
                raise Exception("No se ejecuto la consulta para obtener datos en sql server")
            data['valor'], data['tiempo'] = validate_data(self.df_data, data, self.LOG, col)

        elif fuente.nombre == 2:
            # ORACLE
            # query

            final_query = apifil.get_filter_query(columns, self.nombre_tabla, fuente.nombre) + ';'

            where_query = final_query.split('FROM')[1]
            #
            where_query_f = f"SELECT COUNT(*) FROM" + where_query
            #
            try:

                self.num_register = ejecutar_oracle(self.db_info, where_query_f[:-1], params=None)

            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'El Query para la Consulta en la Base de datos ORACLE con la información del cliente no cumple con los estándares establecidos.El archivo de Excel no cumple con la estructura/formato estándar definido.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Obtencion numero registros |Cargue de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_clean(error)
                raise Exception("No se encontraron registros")
            if int(self.num_register[0][0]) == 0:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'El filtro aplicado no obtiene datos',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Obtencion datos, no se encuentran registros|Cargue de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_clean(error)
                raise Exception('Error el filtro aplicado no obtiene registros entre los datos')

            try:
                list_data = cargue_oracle(self.db_info, final_query[:-1], self.num_register[0][0], params=None,
                                          unique_id=unique_id, user_id=user_id)
            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'El Query para la Consulta en la Base de datos ORACLE con la información del cliente no cumple con los estándares establecidos.El archivo de Excel no cumple con la estructura/formato estándar definido.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Cargue de datos',
                    'archivo_log': 'NO'

                }
                #
                self.LOG.append_clean(error)
                raise Exception("No se ejecuto la consulta para ORACLE")
            # self.df_data=self.obtain_file(user_id,load_data_struc)
            list_data

            self.df_data = pd.concat(list_data, ignore_index=True, axis=0)
            self.df_data = self.df_data[col]
            self.df_data.reset_index()

            data['valor'], data['tiempo'] = validate_data(self.df_data, data, self.LOG, col)

        elif fuente.nombre == 3:
            # XLSX

            data['ruta'] = self.db_info['ruta']
            try:
                self.df_data = cargue_xlsx(self.db_info['ruta'], load_data_struc)
                df_data2 = pd.read_excel(self.db_info['ruta'])


            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'El archivo de Excel con la información del cliente no cumple con los estándares establecidos.El archivo de Excel no cumple con la estructura/formato estándar definido.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Cargue de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_clean(error)

            data['valor'], data['tiempo'] = validate_data(self.df_data, data, self.LOG, col)
            # aplicar filtros
            self.df_data = apifil.apply_filter_to_dataframe(self.df_data, columns)

        elif fuente.nombre == 4:
            '''cargue con csv'''

            data['ruta'] = self.db_info['ruta']

            try:
                self.df_data = cargue_csv(self.db_info['ruta'], load_data_struc)

            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'El archivo de CSV con la información del cliente no cumple con los estándares establecidos.El archivo de Excel no cumple con la estructura/formato estándar definido.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Cargue de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_clean(error)
                raise ()

            data['valor'], data['tiempo'] = validate_data(self.df_data, data, self.LOG, col)

            # aplicar filtros
            self.df_data = apifil.apply_filter_to_dataframe(self.df_data, columns)

        else:

            try:
                False
            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': 'api_paramNorm',
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'Fuente de datos no definida.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Cargue de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_clean(error)
                raise ()

        return data

    def output(self):
        pass

    def obtain_file(self, user_id, load_data_struc):
        data = pd.DataFrame()

        ruta = (f'media/{user_id}/input/')

        csv_files = glob.glob(f'{ruta}*.csv')
        # Mostrar el archivo csv_files, el cual es una lista de nombres

        list_data = []
        for filename in csv_files:
            try:
                list_data.append(cargue_csv(filename, load_data_struc, low_memory=False))

            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': 'api_paramNorm',
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'El archivo de CSV con la información del cliente no cumple con los estándares establecidos.El archivo de Excel no cumple con la estructura/formato estándar definido.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Cargue de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_clean(error)
                raise ()

        '''Para chequear que todo está bien, mostramos la list_data por consola'''
        list_data

        data = pd.concat(list_data, ignore_index=True)

        return data

    def save_database(self, data):

        table_name = self.nombre_destino
        # self.df_data.reset_index()
        self.df_data['ID'] = data['unique_id']

        a = self.df_data.columns
        cols_and_types = ''
        only_cols = ''
        number_cols = ''
        i = 1

        len_data = len(self.df_data[a[0]])
        for column in a:

            if column == 'id':
                cols_and_types += f"index INT PRIMARY KEY NOT NULL, "
            else:
                cols_and_types += f"{str(column)} TEXT, "
            only_cols += f"{str(column)}, "
            number_cols += f":{i}, "
            i = 1 + i
        cols_and_types = cols_and_types[:len(cols_and_types) - 2]

        only_cols = only_cols[:len(only_cols) - 2]
        number_cols = number_cols[:len(number_cols) - 2]
        b = f'CREATE TABLE {table_name} ({cols_and_types});'
        #




        if self.tipo_fuente == 1:
            '''try:
                self.table = ejecutar_sqlserver(self.db_info, b, params=None)
                print("numregister", self.table)
            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'El Query para la Consulta en la Base de datos ORACLE con la información del cliente no cumple con los estándares establecidos.El archivo de Excel no cumple con la estructura/formato estándar definido.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Obtencion numero registros |Cargue de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_exec(error)
                raise ()
            '''
            try:
                guardar_sqlserver(self.db_info, self.df_data, len_data, table_name)

            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'Error al guardar los datos en la BASE DE DATOS  definida.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Envio de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_exec(error)
                raise ()

        elif self.tipo_fuente == 2:
            try:
                guardar_oracle(self.db_info, self.df_data, len_data, table_name)

            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'Error al guardar los datos en la BASE DE DATOS  definida.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Envio de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_exec(error)
                raise ()

        # a = f"insert into {table_name} ({only_cols}) values ({number_cols})"

        # guardar_sqlserver({},self.df_data,len_data,data['unique_id'])
        # guardar_oracle({},self.df_data,len_data,self.data['unique_id'])

    def save_file(self, data, columns_primarias, agrupaciones, df_data):

        df_data = df_data[columns_primarias + agrupaciones]
        nombre_archivo = str(data['ruta'])

        df_data = pd.melt(df_data, id_vars=columns_primarias, var_name=data['tiempo'],
                          value_name=data['valor'])
        df_data['ID'] = data['unique_id']
        if '.xlsx' in str(nombre_archivo):

            #
            nombre_arc = nombre_archivo[:len(nombre_archivo) - 5]
            #
            try:
                df_data.to_excel(nombre_arc + "_out.xlsx")

            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'Error al guardar el archivo en formato XLSX definido.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Cargue de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_exec(error)
                raise ()

        elif ".csv" in nombre_archivo:
            #
            nombre_arc = nombre_archivo[:len(nombre_archivo) - 4]
            try:
                df_data.to_csv(nombre_arc + "_out.csv")

            except:
                error = {
                    'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'Api_Catalogo': data['api_paramNorm'],
                    'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                    'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                    'desc_error_usuario': 'Error al guardar el archivo en formato CSV definido.',
                    'tipo_error': 'CONTROLADO',
                    'nivel_error': 'ERROR',
                    'proceso': 'Cargue de datos',
                    'archivo_log': 'NO'

                }

                self.LOG.append_exec(error)
                raise ()

    def normalize(self, data, user={}):
        """
        1. conectarse a la fuente de datos
        2. aplicar filtros a la fuente de datos
        3. hacer las operaciones
        4. generar una salida
        """

        self.LOG = logs_normalizacion.get_interactive_log(user_id=str(user.id))
        data['unique_id'] = str(uuid.uuid1())
        print("data",data)
        data_new = self.datasource_conection(data, user.id)

        try:
            normalize = Normalize(data=data_new, dataframe=self.df_data, LOG=self.LOG)
            normalize_df, colum_primary, agrupaciones = normalize.get_normalize()
        except:
            error = {
                'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'Api_Catalogo': data['api_paramNorm'],
                'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                'desc_error_usuario': 'Error al Normalizar los DATOS.',
                'tipo_error': 'CONTROLADO',
                'nivel_error': 'ERROR',
                'proceso': 'Envio de datos',
                'archivo_log': 'NO'

            }

            self.LOG.append_exec(error)
            raise ()

        if self.tipo_fuente == 1 or self.tipo_fuente == 2:

            self.save_database(data=data_new)
        elif self.tipo_fuente == 3 or self.tipo_fuente == 4:

            self.save_file(data=data_new, columns_primarias=colum_primary, agrupaciones=agrupaciones,
                           df_data=normalize_df)

        return data['unique_id']
