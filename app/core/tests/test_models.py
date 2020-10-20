from unittest.mock import patch

from django.test import TestCase
from django.contrib.auth import get_user_model
from random import randint

from normaliza import models


def sample_user(email='test123@globalbit.com', password='testpass'):
    """Create a sample user"""
    return get_user_model().objects.create_user(email, password)



def sample_tipofuente(nombre=1):
    return models.TipoFuente.objects.create(nombre=nombre)

def sample_apicatalogo(nombre='Normalizando'):
    return models.ApiCatalogo.objects.create(nombre=nombre)

def sample_normalizacion():
    params_normalizacion = models.Normalizacion.objects.create(

        user=sample_user(email='pruebas3@globalbit.com'),
        api_paramNorm=sample_parametrosnorm(),
        nombre_fuente='tablaa23',
        fecha_inicio='201911',
        fecha_fin='202004',
        periodo_salida=1,
        prom_periodo=4,
        vector_promedio=1,
        semanas=100,
        meses=12,



    )
    return params_normalizacion


def sample_parametrosnorm():
    tipofuente = sample_tipofuente()
    apicatalogo = sample_apicatalogo()

    api_param_norm = models.ParametrosNorm.objects.create(
        nombre='archivo1.xlsx',
        Fuente=tipofuente,
        user=sample_user(email='pruebas31@globalbit.com'),

        ApiCatalogo=apicatalogo
    )
    return api_param_norm



def sample_columna():
    columna = models.Columna.objects.create(
        user=sample_user(email='pruebascol'+str(randint(0,1000))+'umnas1@globalbit.com'),
        nombre_columna="CODIGO TQ",
        normalizacion=sample_normalizacion(),
        mapeo='COD TQ',
        tipo='int',
    )
    return columna

def sample_filtro():
    filtro = models.Filtro.objects.create(
        user=sample_user(email='pruebasfiltro' + str(randint(0, 1000)) + '1@globalbit.com'),
        columna=sample_columna(),
        operador='=',
        mapeo='COD TQ',
        tipo_valor='int',
    )
    return filtro

class ModelTests(TestCase):

    def test_create_user_with_email_successful(self):
        """Test crear un nuevo usuario con autenticacion"""
        email = 'test@globalbit.com'
        password = 'Testpass123'
        user = get_user_model().objects.create_user(
            email=email,
            password=password
        )

        self.assertEqual(user.email, email)
        self.assertTrue(user.check_password(password))

    def test_new_user_email_normalized(self):
        """prueba de correo normalizado"""
        email = 'test@globalbit.COM'
        user = get_user_model().objects.create_user(email, 'test123')

        self.assertEqual(user.email, email.lower())

    def test_new_user_invalid_email(self):
        """test de crear un usuario con error por falta de correo"""
        with self.assertRaises(ValueError):
            get_user_model().objects.create_user(None, 'test123')

    def test_create_new_superuser(self):
        """Test de creacion de superuser"""
        user = get_user_model().objects.create_superuser(
            'test@globalbit.com',
            'test123'
        )

        self.assertTrue(user.is_superuser)
        self.assertTrue(user.is_staff)


    def test_tipo_fuente_str(self):
        """test tipo de fuente"""
        tipo_fuente = models.TipoFuente.objects.create(
            nombre=1
        )
        print(tipo_fuente)
        self.assertEqual(tipo_fuente, tipo_fuente.nombre)

    def test_api_catologo_str(self):
        """test api catalogo"""
        api_catalogo = models.ApiCatalogo.objects.create(
            nombre='Normalizacion'
        )

        self.assertEqual(str(api_catalogo), api_catalogo.nombre)

    def test_api_user_param_str(self):
        """test de api user parametros """

        apicatalogo=sample_apicatalogo()

        api_user_param=models.ApiUsuarioParametro.objects.create(

            user=sample_user(email='pruebas33@globalbit.com'),
            Api_catalogo=apicatalogo
        )

        self.assertEqual(str(api_user_param), "usuario :{0} catalogo {1}".format(str(api_user_param.user),str(api_user_param.Api_catalogo)))


    def test_api_param_norm_str(self):
        """testde parametros de la normalizacion """
        tipofuente= sample_tipofuente()
        apicatalogo = sample_apicatalogo()

        api_param_norm= models.ParametrosNorm.objects.create(
            nombre='archivo1.xlsx',
            Fuente=tipofuente,
            user=sample_user(email='juansuva@globalbit.co'),

            ApiCatalogo = apicatalogo
        )

        self.assertEqual(str(api_param_norm), api_param_norm.nombre)

    def test_params_bd(self):
        """test de los parametros de las bases de datos"""
        params_bd= models.ParametrosBD.objects.create(
            parametrosnorm=sample_parametrosnorm(),
            protocolo='tcp',
            host='127.0.0.1',
            puerto = '3600',
            SID= 'Contest',
            usuario ='normlab',
            password = 'password',
            user=sample_user(email='juansuva2@globalbit.co')

        )

        self.assertEqual(str(params_bd), params_bd.host)

    def test_normalizacion(self):
        """test de los parametros de normalizacion por ejecucion"""
        params_normalizacion= models.Normalizacion.objects.create(
            user=sample_user(email='pruebas334@globalbit.com'),


            api_paramNorm = sample_parametrosnorm(),

            nombre_fuente = 'tablaa23',
            fecha_inicio = '201911',
            fecha_fin = '202004',
            periodo_salida = 1,
            prom_periodo = 4,
            vector_promedio = 1,
            semanas = 100,
            meses = 12
        )

        self.assertEqual(str(params_normalizacion), params_normalizacion.nombre_fuente)

    def test_semanas_str(self):
        """test para probar las semanas y el periodo"""
        semanas = models.Semana.objects.create(
            periodo="202010",
            semanas =4,
            normalizacion = sample_normalizacion(),
        )
        self.assertEqual(str(semanas), semanas.periodo)

    def test_columnas_str(self):
        """test para la creacion de las columnas"""
        columna = models.Columna.objects.create(
            user=sample_user(email='pruebascolumna@globalbit.com'),
            nombre_columna="CODIGO TQ",
            normalizacion = sample_normalizacion(),
            mapeo= 'COD TQ',
            tipo= 1,
        )
        self.assertEqual(str(columna), columna.nombre_columna)









