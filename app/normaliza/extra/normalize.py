import pandas as pd
import numpy as np
import sys
import datetime

class Normalize:
    columns = []
    data = {}
    df_data = pd.DataFrame()
    df_data2 = pd.DataFrame()
    upper_limit = -1
    lower_limit = -1
    factor = 52 / 12
    agrupaciones = []
    columns_primarias = []
    LOG=""

    def __init__(self, data, dataframe=pd.DataFrame(),LOG=""):
        self.data = data
        self.df_data = dataframe
        self.columns = []
        self.df_data2 = pd.DataFrame()
        self.upper_limit = -1
        self.lower_limit = -1
        self.factor = 52 / 12
        self.agrupaciones = []
        self.columns_primarias = []
        self.LOG=LOG

    def get_normalize(self):
        self.operate()


        return self.df_data, self.columns_primarias, self.agrupaciones

    def validate_time_period(self):
        """
        En des uso
        """
        dates = self.df_data[self.data['tiempo']].unique()
        if int(self.data['vector_promedio']) > 0:
            pivot_index = np.where(dates == int(self.data['fecha_fin']))[0]
            if len(pivot_index) == 0:
                pivot_index = np.inf
            else:
                pivot_index = pivot_index[0]
            pivot_index = pivot_index + self.data['prom_periodo']
        else:
            pivot_index = np.where(dates == int(self.data['fecha_inicio']))[0]
            if len(pivot_index) == 0:
                pivot_index = np.NINF
            else:
                pivot_index = pivot_index[0]
            pivot_index = pivot_index - self.data['prom_periodo']
        if (pivot_index < 0) or (pivot_index > len(dates)):
            return False
        else:
            if int(self.data['vector_promedio']) > 0:
                self.upper_limit = dates[pivot_index]
                self.lower_limit = int(self.data['fecha_fin'])
            else:
                self.upper_limit = int(self.data['fecha_inicio'])
                self.lower_limit = dates[pivot_index]
        return True

    def get_sliced_frame(self):
        """
        Obtener el data frame en el rango de indices
        """

        self.df_data[self.data['tiempo']] = self.df_data[self.data['tiempo']].astype(np.int64)
        return self.df_data[(self.df_data[self.data['tiempo']] >= int(self.lower_limit)) &
                            (self.df_data[self.data['tiempo']] <= int(self.upper_limit))]

    def check_if_period_exist(self, year, month):
        period = str(year) + (str(month) if month > 9 else '0' + str(month))
        return False

    def add_period(self):
        """
        En caso de que algun periodo de tiempo no se encuentre
        En el data frame aqui se agrega al mismo y se igualan
        todos sus valores a 0
        """
        pass

    def get_limits(self):
        """
        Calcula el periodo de tiempo en el que
        se tienen que hacer los calculos basados
        en n-1 periodos de tiempo
        """
        if int(self.data['vector_promedio']) > 0:
            upper = int(self.data['fecha_fin'])
            n = self.data['prom_periodo']  # se resta uno en el for
            limit = str(upper)
            year = int(limit[:4])
            month = int(limit[4:])
            for i in range(1, n):
                month = month + 1
                if month > 12:
                    month = 1
                    year = year + 1
            self.upper_limit = str(year) + (str(month) if month > 9 else '0' + str(month))
            self.lower_limit = int(self.data['fecha_inicio'])
        else:
            lower = int(self.data['fecha_inicio'])
            n = self.data['prom_periodo']  # se resta uno en el for
            limit = str(lower)
            year = int(limit[:4])
            month = int(limit[4:])
            for i in range(1, n):
                month = month - 1
                if month <= 0:
                    month = 12
                    year = year - 1
            self.lower_limit = str(year) + (str(month) if month > 9 else '0' + str(month))
            self.upper_limit = int(self.data['fecha_fin'])

    def get_pivot(self):
        columns = []
        for col in self.data['columnas']:
            if col['tipo_columna'] == 2 or col['tipo_columna'] == 3:
                continue
            columns.append(col['mapeo'])

        return self.df_data.pivot_table(self.data['valor'], columns,
                                        self.data['tiempo'], aggfunc={self.data['valor']: "sum"}).fillna(
            0).reset_index()

    def get_factor(self):
        semanas = self.data['semanas']
        meses = self.data['meses']
        self.factor = int(semanas) / int(meses)


    def complet_month(self):
        self.lower_limit = str(self.lower_limit)
        self.upper_limit = str(self.upper_limit)
        year = int(self.lower_limit[:4])
        month = int(self.lower_limit[4:])
        actual = self.lower_limit
        while int(actual) <= int(self.upper_limit):
            self.columns.append([int(actual), 0])

            if int(actual) not in self.df_data.columns:
                self.df_data[int(actual)] = 0
            month = month + 1
            if month > 12:
                month = 1
                year = year + 1
            actual = str(year) + (str(month) if month > 9 else '0' + str(month))

    def get_weeks(self):

        for dato in self.data['semanas_esp']:
            for periodo in self.columns:
                if int(periodo[0]) == int(dato['periodo']):
                    periodo[1] = dato['semanas']
                    break

    def normalize_data(self):

        # asc
        if self.data['vector_promedio'] == -1:

            # mesual

            n = int(self.data['prom_periodo'])
            a = self.columns

            algo = len(a)

            for i in range(algo - 1, -1, -1):

                suma = 0  # pd.DataFrame()
                suma_semanas = 0

                for j in range(i - (n - 1), i + 1):
                    suma_semanas += self.columns[j][1]

                    suma += self.df_data[self.columns[j][0]]

                name_colum = "n" + str(self.columns[i][0])
                name_colum_sem = "s" + str(self.columns[i][0])
                name_colum_final = "f" + str(self.columns[i][0])
                self.df_data[name_colum] = suma

                self.df_data[name_colum_sem] = suma / suma_semanas
                self.df_data[name_colum_final] = suma / suma_semanas * self.factor



        # desc
        else:

            # mesual

            n = int(self.data['prom_periodo'])
            a = self.columns

            algo = len(a) - (n-1)


            for i in range(0, algo):

                suma = 0  # pd.DataFrame()
                suma_semanas = 0

                for j in range(i, i + n):

                    if (self.columns[j][1] == 0):
                        self.columns[j][1] = 4
                    suma_semanas += self.columns[j][1]

                    suma += self.df_data[self.columns[j][0]]

                name_colum = "n" + str(self.columns[i][0])
                name_colum_sem = "s" + str(self.columns[i][0])
                name_colum_final = "f" + str(self.columns[i][0])
                self.df_data[name_colum] = suma

                self.df_data[name_colum_sem] = suma / suma_semanas
                self.df_data[name_colum_final] = suma / suma_semanas * self.factor


    def get_columns_primary(self):

        for dato in self.data['columnas']:
            if dato['tipo_columna'] == 2 or dato['tipo_columna'] == 3:
                continue
            self.columns_primarias.append(dato['mapeo'])







    def calculartiempo(self):


        cero=""
        if self.data['periodo_salida'] == 1:
            cero='0'



            # trimestral

        year = int(self.lower_limit[:4])
        month = int(self.lower_limit[4:])
        actual = self.lower_limit
        periodo = {}
        tiempo = self.data['periodo_salida']


        while int(actual) != (int(self.upper_limit) - (int(self.data['prom_periodo'])-1) ):
            month_actual = str((month - 1) // tiempo + 1)
            mes_actual=(month_actual.zfill(2) if self.data['periodo_salida'] == 1 else month_actual)

            escala_tiempo = str(actual[:4]) + mes_actual
            if escala_tiempo not in self.df_data.columns:
                self.agrupaciones.append(escala_tiempo)
                # agregamos el valor actual


                self.df_data[escala_tiempo] = self.df_data['f' + actual]

            else:

                self.df_data[escala_tiempo] += self.df_data['f' + actual]

            # a cual pertenece y sumarlos

            month = month + 1
            if month > 12:
                month = 1
                year = year + 1
            escala_old = escala_tiempo
            actual = str(year) + (str(month) if month > 9 else '0' + str(month))




    def operate(self):


        #self.df_data = self.df_data.sort_values(by=[self.data['tiempo']], ascending=True, kind='mergesort')

        try:
            self.get_limits()
        except:
            error = {
                'fecha': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'Api_Catalogo': 'api_paramNorm',
                'error_tecnico': str(sys.exc_info()[0]).replace('\'', '*'),
                'desc_error_tecnico': str(sys.exc_info()[1:]).replace(',', '|').replace('\'', '*'),
                'desc_error_usuario': 'Los liminites para operar estan fuera del rango.',
                'tipo_error': 'CONTROLADO',
                'nivel_error': 'ERROR',
                'proceso': 'Cargue de datos',
                'archivo_log': 'NO'

            }

            self.LOG.append_exec(error)
            raise ()





        self.df_data = self.get_sliced_frame()

        self.df_data = self.get_pivot()

        self.complet_month()

        self.get_weeks()

        self.get_factor()

        self.normalize_data()

        self.calculartiempo()

        self.get_columns_primary()






        # if not self.validate_time_period():
        #    raise Exception('Error Periodos insuficientes')
        # self.df_data = self.get_sliced_frame()
        # Exception('Falso error, es para no crear registro en base de datos')
