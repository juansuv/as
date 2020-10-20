import tempfile
import os
import json


from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from rest_framework import status
from rest_framework.test import APIClient

from normaliza.models import ApiCatalogo, TipoFuente, ParametrosNorm, \
    ParametrosBD,Normalizacion, Columna, Filtro


from normaliza.serializers import NormalizacionSerializer, NormalizacionDetailSerializer, \
    FiltroSerializer, FiltroDetailSerializer





NORMALIZACION_URL = reverse('normaliza:normalizacion-list')

def sample_apicatalogo(nombre='Normalizando'):
    return ApiCatalogo.objects.create(nombre=nombre)

def sample_tipofuente(nombre=4):
    return TipoFuente.objects.create(nombre=nombre)


def detail_url(paramtrosnorm_id):
    """Return Normalizacion detail URL"""
    return reverse('normaliza:normalizacion-detail', args=[paramtrosnorm_id])




def sample_paramnorm(user, **params):
    """Create and return a sample Normalizacion"""
    defaults = {
        'ApiCatalogo': sample_apicatalogo(),
        'Fuente': sample_tipofuente(),
        'nombre': 'archivo1.txt',


    }
    defaults.update(params)

    return ParametrosNorm.objects.create(user=user, **defaults)



def sample_paramdb(user, **params):
    """Create and return a sample Normalizacion"""
    defaults = {
        'parametrosnorm': sample_paramnorm(user=user),
        'protocolo': 'tcp',
        'host': 'locahost',
        'puerto': '3096',
        'SID': 'Database',
        'ruta': 'Archivolugarubicacion.xlsx', #??
        'usuario': 'userdbtest',
        'password' : 'pruebas123'
    }

    defaults.update(params)

    return ParametrosBD.objects.create(user=user, **defaults)

def sample_normalizacion(user, **params):
    """Create and return a sample Normalizacion"""
    defaults = {

        'api_paramNorm' : sample_paramnorm(user),
        'nombre_fuente' : 'tablaa23',
        'fecha_inicio' : '201911',
        'fecha_fin' : '202004',
        'periodo_salida' : 1,
        'prom_periodo' : 4,
        'vector_promedio' : 1,
        'semanas' : 100,
        'meses' : 12
    }
    print("\n\n\n asda",defaults)
    defaults.update(params)

    return Normalizacion.objects.create(user=user, **defaults)


def sample_columna(user,**params):
    """Create and return a sample Columna"""
    defaults = {
        'nombre_columna' : 'COD TQ',
        'normalizacion' : sample_normalizacion(user),
        'mapeo' : 'CODIGO TQ',
        'tipo' : 2,

    }

    defaults.update(params)

    return Columna.objects.create(user=user,**defaults)

def sample_filtro(user,**params):
    """Create and return a sample filtro"""
    defaults = {
        'columna' : sample_columna(user=user),
        'operador' : '>=',
        'tipo_valor' : 2,
        'valor_filtro' : 'filtroosample'

    }

    defaults.update(params)

    return Filtro.objects.create(user=user,**defaults)

class PublicNormalizacionApiTests(TestCase):
    """Test unauthenticated Normalizacion API access"""

    def setUp(self):
        self.client = APIClient()

    def test_auth_required(self):
        """Test that authentication is required"""
        res = self.client.get(NORMALIZACION_URL)

        self.assertEqual(res.status_code, status.HTTP_401_UNAUTHORIZED)


class PrivateNormalizacionApiTests(TestCase):
    """Test unauthenticated Normalizacion API access"""

    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            'testnormalizacion@globalbit.co',
            'testpass'
        )
        self.client.force_authenticate(self.user)

    def test_retrieve_Normalizacion(self):
        """Test retrieving a list of Normalizacion"""
        sample_normalizacion(user=self.user)
        sample_normalizacion(user=self.user)

        res = self.client.get(NORMALIZACION_URL)

        normalizacion = Normalizacion.objects.all().order_by('-id')
        serializer = NormalizacionSerializer(normalizacion, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(res.data, serializer.data)


    def test_Normalizacion_limited_to_user(self):
        """Test retrieving Normalizacion for user"""
        user2 = get_user_model().objects.create_user(
            'othernormaliza@globalbit.co',
            'password123'
        )
        sample_normalizacion(user=user2)
        sample_normalizacion(user=self.user)

        res = self.client.get(NORMALIZACION_URL)

        normalizacion = Normalizacion.objects.order_by('-id')
        serializer = NormalizacionSerializer(normalizacion, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(len(res.data), 2)
        self.assertEqual(res.data, serializer.data)

    '''
    def test_view_parambd_detail(self):
        
        paramdb = sample_paramdb(user=self.user)
        paramdb.puerto= '1234'
        paramdb.host= 'localtest'
        paramdb.parametrosnorm=sample_paramnorm(user=self.user)


        url = detail_url(paramdb.id)
        res = self.client.get(url)

        serializer = ParamBDDetailSerializer(paramdb)
        self.assertEqual(res.data, serializer.data)
    '''


    def test_create_parambd_with_Normalizacion(self):
        """Test parambd a Normalizacion with Normalizacion"""
        normalizacion = sample_normalizacion(user=self.user)
        paramnorm = sample_paramnorm(user=self.user)

        payload =    {
      "api_paramNorm": 1,
      "nombre_fuente": "nomalizacion primera ALPINE",
      "fecha_inicio": "202003",
      "fecha_fin": "202012",
      "periodo_salida": 2,
      "prom_periodo": 3,
      "vector_promedio": 1,
      "semanas": 23,
      "meses": 12,
      "valor": "COD TQ ",
      "columnas": [
              {
                  'tipo_columna': 3,
                  "nombre_columna": "ALPINE",
                  "mapeo": "ALPINE!",
                  "tipo": 1,
                  "filtros":
                  [
                    {
                      "operador": "==",
                      "tipo_valor": 1,
                      "valor_filtro": "13"
                    },
                    {
                      "operador": "!=",
                      "tipo_valor": 1,
                      "valor_filtro": "13"
                    }
                  ]
              },
            {

                'tipo_columna': 2,
                  "nombre_columna": "ALPINE2",
                  "mapeo": "ALPINE2!",
                  "tipo": 1,
                  "filtros":
                  [
                  ]
              }
          ]
      }
        res = self.client.post(NORMALIZACION_URL, payload, format='json')

        self.assertEqual(res.status_code, status.HTTP_201_CREATED)
        normalizacion = Normalizacion.objects.get(id=res.data['id'])
        nombrefuentenorm = normalizacion.nombre_fuente
        self.assertEqual(len(res.data), 11)
        self.assertEqual(normalizacion.nombre_fuente, nombrefuentenorm)


    def test_updatee_api_user_with_apicactologo(self):

        normalizacion = sample_normalizacion(user=self.user)
        normalizacion.parametrosnorm = sample_paramnorm(user=self.user)

        new_paramnorm = sample_paramnorm(user=self.user)

        payload = { 'api_paramNorm': new_paramnorm.id, 'nombre_fuente':'2323'}
        url = detail_url(normalizacion.id)
        
        self.client.patch(url, payload)

        normalizacion.refresh_from_db()
        self.assertEqual(normalizacion.nombre_fuente, payload['nombre_fuente'])


    def test_Normalizacion_with_columnas(self):
        """Test retrieving a list of Normalizacion"""
        normalizacion1=sample_normalizacion(user=self.user, fecha_inicio='199912')

        sample_columna(user=self.user,nombre_columna='pruebas12',mapeo='pruebasexito12123',normalizacion=normalizacion1)
        sample_columna(user=self.user,nombre_columna='pruebas32',mapeo='pruebasexito123',normalizacion=normalizacion1)
        sample_columna(user=self.user,nombre_columna='pruebas42',mapeo='pruebasexito12', normalizacion=normalizacion1)
        res = self.client.get(NORMALIZACION_URL)

        normalizacion = Normalizacion.objects.all().order_by('-id')
        serializer = NormalizacionSerializer(normalizacion, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(res.data, serializer.data)


    def test_Normalizacion_with_columnas_and_filtros(self):
        """Test retrieving a list of Normalizacion"""
        normalizacion1=sample_normalizacion(user=self.user)

        columna1=sample_columna(user=self.user,nombre_columna='pruebas12',mapeo='pruebasexito12123',normalizacion=normalizacion1)
        columna2=sample_columna(user=self.user,nombre_columna='pruebas32',mapeo='pruebasexito123',normalizacion=normalizacion1)
        columna3=sample_columna(user=self.user,nombre_columna='pruebas42',mapeo='pruebasexito12', normalizacion=normalizacion1)

        sample_filtro(user=self.user, columna=columna1, operador='>', tipo_valor=2)
        sample_filtro(user=self.user, columna=columna2, operador='<=', tipo_valor=2)
        sample_filtro(user=self.user, columna=columna3, operador='in', tipo_valor=2)

        res = self.client.get(NORMALIZACION_URL)

        normalizacion = Normalizacion.objects.all().order_by('-id')
        serializer = NormalizacionSerializer(normalizacion, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(res.data, serializer.data)

    def test_Normalizacion_with_columnas_and_filtros_and_valoresFiltro(self):
        """Test retrieving a list of Normalizacion"""
        normalizacion1=sample_normalizacion(user=self.user)

        columna1=sample_columna(user=self.user,nombre_columna='pruebas12',mapeo='pruebasexito12123',tipo=1,normalizacion=normalizacion1)
        columna2=sample_columna(user=self.user,nombre_columna='pruebas32',mapeo='pruebasexito123',tipo=2,normalizacion=normalizacion1)
        columna3=sample_columna(user=self.user,nombre_columna='pruebas42',mapeo='pruebasexito12',tipo=5, normalizacion=normalizacion1)
        columna3=sample_columna(user=self.user,nombre_columna='pruebas42',mapeo='pruebasexito12',tipo=5, normalizacion=normalizacion1)

        filtro1 = sample_filtro(user=self.user, columna=columna1, operador='>', tipo_valor=1)
        filtro2 = sample_filtro(user=self.user, columna=columna2, operador='<=', tipo_valor=1)
        filtro3 = sample_filtro(user=self.user, columna=columna3, operador='in', tipo_valor=2)


        sample_paramdb(user=self.user,parametrosnorm=normalizacion1.api_paramNorm)

        res = self.client.get(NORMALIZACION_URL)

        normalizacion = Normalizacion.objects.all().order_by('-id')
        serializer = NormalizacionSerializer(normalizacion, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(res.data, serializer.data)