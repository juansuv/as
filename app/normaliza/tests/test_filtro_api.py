import tempfile
import os

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from rest_framework import status
from rest_framework.test import APIClient

from normaliza.models import ApiCatalogo, TipoFuente, ParametrosNorm, \
    ParametrosBD, Normalizacion, Columna, Filtro

from normaliza.serializers import NormalizacionSerializer, NormalizacionDetailSerializer, \
    FiltroSerializer, FiltroDetailSerializer

FILTRO_URL = reverse('normaliza:filtro-list')


def sample_apicatalogo(nombre='Normalizando'):
    return ApiCatalogo.objects.create(nombre=nombre)


def sample_tipofuente(nombre=1):
    return TipoFuente.objects.create(nombre=nombre)


def detail_url(filtro_id):
    """Return Filtro detail URL"""
    return reverse('normaliza:filtro-detail', args=[filtro_id])


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
        'ruta': 'Archivo.xlsx',  # ??
        'usuario': 'userdbtest',
        'password': 'pruebas123'
    }

    defaults.update(params)

    return ParametrosBD.objects.create(user=user, **defaults)


def sample_normalizacion(user, **params):
    """Create and return a sample Normalizacion"""
    defaults = {
        'api_paramNorm': sample_paramnorm(user=user),
        'nombre_fuente': 'tablaa23',
        'fecha_inicio': '201911',
        'fecha_fin': '202004',
        'periodo_salida': 1,
        'prom_periodo': 4,
        'vector_promedio': 1,
        'semanas': 100,
        'meses': 12
    }

    defaults.update(params)

    return Normalizacion.objects.create(user=user, **defaults)


def sample_columna(user, **params):
    """Create and return a sample Columna"""
    defaults = {
        'nombre_columna': 'COD TQ',
        'normalizacion': sample_normalizacion(user),
        'mapeo': 'CODIGO TQ',
        'tipo': 1,

    }

    defaults.update(params)

    return Columna.objects.create(user=user, **defaults)


def sample_filtro(user, **params):
    """Create and return a sample Columna"""
    defaults = {
        'columna': sample_columna(user=user),
        'operador': '>=',
        'tipo_valor': 1,
        'valor_filtro' : "sample_filtre"
    }

    defaults.update(params)

    return Filtro.objects.create(user=user, **defaults)


class PublicFiltroApiTests(TestCase):
    """Test unauthenticated Filtro API access"""

    def setUp(self):
        self.client = APIClient()

    def test_auth_required(self):
        """Test that authentication is required"""
        res = self.client.get(FILTRO_URL)

        self.assertEqual(res.status_code, status.HTTP_401_UNAUTHORIZED)


class PrivateFiltroApiTests(TestCase):
    """Test unauthenticated Filtro API access"""

    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            'testFiltro@globalbit.co',
            'testpass'
        )
        self.client.force_authenticate(self.user)

    def test_retrieve_Columna(self):
        """Test retrieving a list of Columna"""
        sample_filtro(user=self.user)
        sample_filtro(user=self.user)

        res = self.client.get(FILTRO_URL)

        filtro = Filtro.objects.all().order_by('-id')
        serializer = FiltroSerializer(filtro, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(res.data, serializer.data)

    def test_filtro_limited_to_user(self):
        """Test retrieving Filtro for user"""
        user2 = get_user_model().objects.create_user(
            'othercolumna@globalbit.co',
            'password123'
        )
        sample_filtro(user=user2)
        sample_filtro(user=self.user)

        res = self.client.get(FILTRO_URL)

        filtro = Filtro.objects.all().order_by('-id')
        serializer = FiltroSerializer(filtro, many=True)
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

    def test_create_Filtro_with_columna(self):
        """Test parambd a Columna with Normalizacion"""
        columna = sample_columna(user=self.user)

        payload = {
            'columna': columna.id,
            'operador': '>=',
            'tipo_valor': 1,
            'valor_filtro': 'pruebasvalorfltro'
        }
        res = self.client.post(FILTRO_URL, payload)

        self.assertEqual(res.status_code, status.HTTP_201_CREATED)
        filtro = Filtro.objects.get(id=res.data['id'])
        operador = filtro.operador
        self.assertEqual(len(res.data), 5)
        self.assertEqual(payload['operador'], operador)

    def test_updatee_Filtro_with_columna(self):
        filtro = sample_filtro(user=self.user)
        filtro.columna = sample_columna(user=self.user)

        new_columna = sample_columna(user=self.user, mapeo='TQ CODES')

        payload = {'columna': new_columna.id}
        url = detail_url(filtro.id)

        self.client.patch(url, payload)

        filtro.refresh_from_db()
        self.assertEqual(filtro.columna.id, payload['columna'])