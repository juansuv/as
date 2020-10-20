import os


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class LoadFile:
    '''
    Esta estructura se usa para el cargue de archivos XLSX (Excel) o CSV
    :param col: Columnas designadas por obligación para el archivo a cargar
    :param ren: Homologación estandar al nombre se que le brinda a cada columna.
    :param dtype: Se ponen por defecto los tipos de las columnas para optimizar memoria.
    '''
    def __init__(self, col, ren, dtype):
        self.columns = col
        self.columns_rename = ren
        self.columns_dtypes = dtype




class ConnDataBase:
    '''
    Estructura general para conexión a una base de datos
    :param server: Se define el servidor a ejecutar
    :param port: Se define el puerto a conexión
    :param scheme: Se da el nombre del esquema que se está usando
    :param user: Se ingresa el nombre del usuario
    :param password: Se ingresa contraseña del usuario
    '''
    def __init__(self, server, port,sid, scheme, user, password):
        self.server = server
        self.port = port
        self.scheme = scheme
        self.sid = sid
        self.user = user
        self.password = password




load_maestra_articulos_factores = LoadFile(
    col=[
        "COD PRODUCTO",
        "COD BARRAS",
        "DESCRIPCION",
        "COD TQ",
        "FACTOR DE CONVERSION"

    ],
    ren={
        "COD PRODUCTO"      :  "COD PRODUCTO",
        "COD BARRAS"        :  "COD BARRAS",
        "DESCRIPCION"       :  "DESCRIPCION",
        "COD TQ"            :  "COD TQ",
        "FACTOR DE CONVERSION" :  "FACTOR DE CONVERSION"

    },     
    dtype ={
        "COD PRODUCTO"              :  object,
        "COD BARRAS"                :  object,
        "DESCRIPCION"               :  object,
        "COD TQ"                    :  object,
        "FACTOR DE CONVERSION"      :  float


    },


)
