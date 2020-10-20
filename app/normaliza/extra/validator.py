import datetime
import sys
import os


def validate_data(data, normalizacion, LOG, col):
    columns = normalizacion['columnas'].copy()
    # print("columnas", columns)
    for column in columns:
        if column['tipo_columna'] == 3:
            valor = column['nombre_columna']
        elif column['tipo_columna'] == 2:
            tiempo = column['nombre_columna']

    # -----------------
    # EMPTYNESS CHECK
    # se valida si el archivo viene o no vacio
    if data.empty:
        error = {
            'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'Normalizacion': normalizacion['api_paramNorm'],
            'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
            'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
            'desc_error_usuario': 'La maestra no cumple con los estandares definidos.El archivo de Excel viene vacio.',
            'tipo_error': 'CONTROLADO',
            'nivel_error': 'ERROR',
            'proceso': ' | Cargue a sistema',
            'archivo_log': 'NO'

        }
        LOG.append_clean(error)
        print(error)
        # fun_database.subir_info_error(USUARIO_ODS_FINAN_ESCRITURA, error)
        data_validation = False
        raise Exception('Error archivo vacío')
    # Se valida que no existan campos vacíos
    '''data_vta = data[(data['TIPO'] == 'VTA') | (data['TIPO'] == 'VTA NETA')]
    if True in list(data_vta[['COD ESTABLECIMIENTO', 'ESTABLECIMIENTO']].isna().isin([True]).any()):
        error = {
            'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'eds': 'Normalizacion',
            'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
            'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
            'desc_error_usuario': 'La Maestra de Disfor no cumple con los estándares definidos.El archivo de Excel tiene campos vacíos/nulos.',
            'tipo_error': 'CONTROLADO',
            'nivel_error': 'ERROR',
            'proceso': '0. datos_cliente.xlsx  | Cargue a sistema',
            'archivo_log': 'NO'
        }
        LOG.append_clean(error)
        # fun_database.subir_info_error(USUARIO_ODS_FINAN_ESCRITURA, error)
        data_validation = False
        raise Exception('Error archivo con campos nulos')
    '''
    for columna in col:
        if data[columna].isna().isin([True]).any():
            error = {
                'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'Normalizacion': normalizacion['api_paramNorm'],
                'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                'desc_error_usuario': f'La Maestra no cumple con los estándares definidos.El archivo tiene campos vacíos/nulos, en la columna{columna}',
                'tipo_error': 'CONTROLADO',
                'nivel_error': 'ERROR',
                'proceso': ' Cargue a sistema',
                'archivo_log': 'NO'
            }
            LOG.append_clean(error)
            print(error)
            # fun_database.subir_info_error(USUARIO_ODS_FINAN_ESCRITURA, error)
            data_validation = False
            raise Exception('Error archivo  de datos de entrada con campos nulos en la columna: ',columna)

            # data[col].fillna(0, inplace=True)

    if valor not in col:
        error = {
            'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'Normalizacion': normalizacion['api_paramNorm'],
            'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
            'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
            'desc_error_usuario': 'La columna de valor no se encuentra  entre las columnas.',
            'tipo_error': 'CONTROLADO',
            'nivel_error': 'ERROR',
            'proceso': 'Cargue a sistema',
            'archivo_log': 'NO'
        }
        LOG.append_clean(error)
        print(error)
        # fun_database.subir_info_error(USUARIO_ODS_FINAN_ESCRITURA, error)
        data_validation = False
        raise Exception('Error archivo  de datos de entrada sin campo de valor')

    if tiempo not in col:
        error = {
            'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'Normalizacion': normalizacion['api_paramNorm'],
            'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
            'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
            'desc_error_usuario': 'La columna de tiempo no se encuentra  entre las columnas.',
            'tipo_error': 'CONTROLADO',
            'nivel_error': 'ERROR',
            'proceso': 'Cargue a sistema',
            'archivo_log': 'NO'
        }
        LOG.append_clean(error)
        print(error)
        # fun_database.subir_info_error(USUARIO_ODS_FINAN_ESCRITURA, error)
        data_validation = False
        raise Exception('Error archivo  de datos de entrada sin campo de tiempo')

    # -----------------
    # DATA PREPARATION
    # ----------------
    # DATA VALIDATION

    # ----------------

    # data.to_csv(os.path.join(CLEAN_FOLDER, '0. Disfor.csv'), index=False)

    return valor, tiempo
