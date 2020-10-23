import os
from django.db import models
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, \
    PermissionsMixin
from django.conf import settings

from django.core.files.storage import FileSystemStorage

from normaliza.extra.input_struct import LoadFile

from .constants import STATUS_CREATED, STATUS_EXTRACT_RUNNING, STATUS_EXTRACT_SUCCESS, \
    STATUS_EXTRACT_FAILURE, STATUS_API_SUCCESS, STATUS_API_FAILURE, STATUS_API_RUNNING, \
    MONTHS_CHOICES, MINUTES_CHOICES, HOURS_CHOICES, DAYS_OF_WEEK_CHOICES, MONTH_DAYS_CHOICES, \
    SCHEDULED_TASKS_STATUS_CHOICES, SCHEDULED_TASK_ACTIVE

API_NAME = 'generic'





class TipoFuente(models.Model):
    FUENTE_CHOICES = (
        (1, 'SQL SERVER'),
        (2, 'ORACLE'),
        (3, 'XLSX'),
        (4, 'CSV'),

    )

    nombre = models.PositiveIntegerField("Tipo de fuente de datos", default=1, choices=FUENTE_CHOICES)

    def __str__(self):
        return str(self.nombre)



class ApiCatalogo(models.Model):
    nombre = models.CharField('Nombre de la aplicacion', max_length=100)

    def __str__(self):
        return self.nombre


class ApiUsuarioParametro(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, models.CASCADE, verbose_name='Usuario de api catalogo')
    Api_catalogo = models.ForeignKey(ApiCatalogo, models.CASCADE, verbose_name='Api catalogo')

    def __str__(self):
        return "usuario :{0} catalogo {1}".format(self.user, self.Api_catalogo)


class ParametrosNorm(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, models.CASCADE, verbose_name='Usuario  de parametros norm')
    ApiCatalogo = models.ForeignKey(ApiCatalogo, models.CASCADE,
                                    verbose_name='Conjunto de servicios')
    Fuente = models.ForeignKey(TipoFuente, models.CASCADE, verbose_name='Tipo de Fuente')
    nombre = models.CharField(max_length=100, verbose_name='Nombre de parametros para aplicacion')
    nombre_fuente = models.CharField(max_length=100, verbose_name='Nombre de la fuente de datos o tabla',null=True,blank=True)
    nombre_destino = models.CharField(max_length=100, verbose_name='Nombre de el destino de datos o tabla',null=True,blank=True)

    def __str__(self):
        return self.nombre

    def get_parambd(self):
        params = self.parametrosbd_set.all()
        for parambd in params:
            datos = {}
            datos['protocolo'] = parambd.protocolo
            datos['host'] = parambd.host
            datos['puerto'] = parambd.puerto
            datos['SID'] = parambd.SID
            datos['ruta'] = parambd.ruta
            datos['usuario'] = parambd.usuario
            datos['password'] = parambd.password
        return datos


class ParametrosBD(models.Model):

    def user_directory_path(instance, filename):
        path = f"media/{instance.user.id}/input/"
        fs = FileSystemStorage(location=path)
        if fs.exists(filename):

            fs.delete(filename)

        return 'media/{0}/input/{1}'.format(instance.user.id, filename)



    user = models.ForeignKey(settings.AUTH_USER_MODEL, models.CASCADE,
                             verbose_name='Usuario  asociado')
    parametrosnorm = models.ForeignKey(ParametrosNorm, models.CASCADE, verbose_name='Parametros de la normalizacion', related_name='parametrosbd')
    protocolo = models.CharField(max_length=100, verbose_name='protocolo', blank=True, null=True)
    host = models.CharField(max_length=100, verbose_name='Host', blank=True, null=True)
    puerto = models.CharField(max_length=100, verbose_name='Puerto', blank=True, null=True)
    SID = models.CharField(max_length=100, verbose_name='SID', blank=True, null=True)
    ruta = models.FileField(upload_to=user_directory_path, max_length=100, verbose_name='Ruta', blank=True, null=True)
    #ruta = models.FileField( max_length=100, verbose_name='Ruta', blank=True, null=True)
    usuario = models.CharField(max_length=100, verbose_name='Usuario', blank=True, null=True)
    password = models.CharField(max_length=100, verbose_name='Password', blank=True, null=True) #prtegido


    def __str__(self):
        if self.host is None:
            return self.ruta
        return self.host


class Normalizacion(models.Model):
    TIMES_CHOICES = (
        (1, 'MES'),
        (2, 'BIMESTRAl'),
        (3, 'TRIMESTRAS'),
        (4, 'CUATRIMESTRAL'),
        (5, 'QUINQUEMESTRE'),
        (6, 'SEMESTRAl'),
        (7, 'SIETE MESES'),
        (8, 'OCHO MESES'),
        (10, 'DIEZ MESES'),
        (11, 'ONCE MESES'),
        (12, 'ANUAL'),

    )

    VECTOR_CHOICES = (
        (1, 'adelante'),
        (-1, 'atras'),

    )

    user = models.ForeignKey(settings.AUTH_USER_MODEL, models.CASCADE, verbose_name='Usuario  asociado')
    api_paramNorm = models.ForeignKey(ParametrosNorm, on_delete=models.CASCADE, null=False)
    nombre_fuente = models.CharField(verbose_name="Nombre ", max_length=36)
    fecha_inicio = models.CharField(verbose_name="Fecha_inicio", max_length=36)
    fecha_fin = models.CharField(verbose_name="Fecha_fin", max_length=36)
    periodo_salida = models.PositiveIntegerField(default=0, choices=TIMES_CHOICES,
                                                 verbose_name='Periodo de Tiempo')
    # n
    prom_periodo = models.PositiveIntegerField(verbose_name="Periodo de Tiempo de Salida", null=False)
    vector_promedio = models.IntegerField(verbose_name="Vector de promedio",choices=VECTOR_CHOICES,default=0)
    semanas = models.PositiveIntegerField(verbose_name='Semanas')
    meses = models.PositiveIntegerField(verbose_name='Meses')
    uuid = models.CharField(verbose_name="uudi", max_length=100,null=True,blank=True)
    #mirar si se asosia a una columna id de las que se agregaron
    #valor = models.CharField(verbose_name="Columna valor ", max_length=36)
    #tiempo = models.CharField(verbose_name="Columna de tiempo ", max_length=36)
    num_process = models.PositiveIntegerField(verbose_name='Numero de procesos  a ejecutar', default=2)
    num_chunck = models.PositiveIntegerField(verbose_name='Tamaño de datos a ejecutar', default=500000)

    def __str__(self):
        return self.nombre_fuente

    def get_tipo_fuente(self):
        return self.api_paramNorm.Fuente.nombre

    def get_api_catalogo(self):
        return self.api_paramNorm.ApiCatalogo.nombre

    def get_parambd(self):
        return self.api_paramNorm.parametrosbd.all()

    def get_api_param(self):
        datos = {}
        datos['api_param_nombre'] = self.api_paramNorm.nombre
        datos['tipo_fuente'] = self.get_tipo_fuente()
        datos['api_catalogo'] = self.get_api_catalogo()

        parambds = self.get_parambd()

        for parambd in parambds:

            datos['protocolo'] = parambd.protocolo
            datos['host'] = parambd.host
            datos['puerto'] = parambd.puerto
            datos['SID'] = parambd.SID
            datos['ruta'] = parambd.ruta
            datos['usuario'] = parambd.usuario
            datos['password'] = parambd.password


        return datos

    def get_columnas(self):
        col = []
        ren = {}
        fil = {}
        dtype = {}
        filtros= ''
        columnas = self.columnas.all()

        cantidad= len(columnas)
        i=0
        for columna in columnas:

            i+=1
            if i<cantidad:
                consulta=columna.build_querys()
                filtros += str(consulta) + ' AND '
            else:
                filtros += columna.build_querys()

            col.append(columna.nombre_columna)
            if columna.nombre_columna not in ren:
                ren[columna.nombre_columna]=[columna.mapeo]
            else:
                ren[columna.nombre_columna].append(columna.mapeo)
            if columna.nombre_columna not in fil:
                fil[columna.nombre_columna]=[(columna.operador,columna.valor_filtro)]
            else:
                fil[columna.nombre_columna].append((columna.operador,columna.valor_filtro)) 

            dtype[columna.nombre_columna]=columna.tipo

 



        data=LoadFile(
            col,
            ren,
            dtype,
            fil,
        )
        return data


class Semana(models.Model):
    normalizacion = models.ForeignKey(Normalizacion, on_delete=models.CASCADE, null=False, related_name='semanas_esp')
    periodo = models.CharField(verbose_name='Periodos_año_mes', max_length=36)
    semanas = models.IntegerField(verbose_name='Semanas')

    def __str__(self):
        return self.periodo


class Columna(models.Model):
    TIPO_CHOICES = (
        (1, 'int'),
        (2, 'string'),
        (3, 'boolean'),
        (4, 'float'),
    )

    TIPO_COLUMNA_CHOICES = (
        (1, 'informacion'),
        (2, 'tiempo'),
        (3, 'valor'),
    )
    user = models.ForeignKey(settings.AUTH_USER_MODEL, models.CASCADE, verbose_name='Usuario  asociado')
    nombre_columna = models.CharField("Nombre de la Columna", max_length=100)
    normalizacion = models.ForeignKey(Normalizacion, on_delete=models.CASCADE,
                                      blank=False, related_name='columnas')
    mapeo = models.CharField("Mapeo de la Columna", max_length=36)
    # tipo de columna que recibe si es niveles de informacion valor, o tiempo.
    tipo = models.PositiveIntegerField("Tipo de dato de la columna",default=0, choices=TIPO_CHOICES)
    tipo_columna = models.PositiveIntegerField("Tipo de valor de la columna",default=1, choices=TIPO_COLUMNA_CHOICES)


    def __str__(self):
        return self.nombre_columna





class Filtro(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, models.CASCADE, verbose_name='Usuario  asociado')
    columna = models.ForeignKey(Columna, on_delete=models.CASCADE,
                                   blank=False, related_name='filtros')
    operador = models.CharField("operador", max_length=10)
    tipo_valor = models.CharField("Tipo_valor", max_length=20)
    valor_filtro = models.CharField("Valor del filtro", max_length=100)

    def __str__(self):
        return " {0} {1} {2} ".format(self.columna,self.operador,self.valor_filtro)



class tiempo(models.Model):

    agrupacion_tiempo = models.IntegerField("agrupacion_tiempo")
    escala_mensual = models.IntegerField("escala_mensual")
    escala_agrupada = models.IntegerField("escala_agrupada")


    def __str__(self):
        return self.agrupacion_tiempo



class EscalaTiempo(models.Model):

    agrupacion_tiempo = models.IntegerField("agrupacion_tiempo")
    escala_mensual = models.IntegerField("escala_mensual")
    escala_agrupada = models.IntegerField("escala_agrupada")


    def __str__(self):
        return self.agrupacion_tiempo