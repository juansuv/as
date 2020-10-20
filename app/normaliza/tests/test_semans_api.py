import tempfile
import os


from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from rest_framework import status
from rest_framework.test import APIClient

from normaliza.models import ApiCatalogo, TipoFuente, ParametrosNorm, \
    ParametrosBD,Normalizacion, Columna, Semana

from normaliza.serializers import NormalizacionSerializer, NormalizacionDetailSerializer, \
    ColumnaSerializer,ColumnaDetailSerializer, SemanaSerializer


SEMANA_URL = reverse('normaliza:semana-list')

def sample_apicatalogo(nombre='Normalizando'):
    return ApiCatalogo.objects.create(nombre=nombre)

def sample_tipofuente(nombre=1):
    return TipoFuente.objects.create(nombre=nombre)


def detail_url(paramtrosnorm_id):
    """Return Normalizacion detail URL"""
    return reverse('normaliza:columna-detail', args=[paramtrosnorm_id])




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
        'ruta': 'Archivo.xlsx', #??
        'usuario': 'userdbtest',
        'password' : 'pruebas123'
    }

    defaults.update(params)

    return ParametrosBD.objects.create(user=user, **defaults)

def sample_normalizacion(user, **params):
    """Create and return a sample Normalizacion"""
    defaults = {

        'api_paramNorm' : sample_paramnorm(user=user),
        'nombre_fuente' : 'tablaa23',
        'fecha_inicio' : '201911',
        'fecha_fin' : '202004',
        'periodo_salida' : 1,
        'prom_periodo' : 4,
        'vector_promedio' : 1,
        'semanas' : 100,
        'meses' : 12
    }

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


def sample_semanas(user,**params):
    """Create and return a sample Columna"""
    defaults = {



        'normalizacion' : sample_normalizacion(user),
        'periodo' : '202010',
        'semanas' : 5,

    }

    defaults.update(params)

    return Semana.objects.create(**defaults)

class PublicColumnaApiTests(TestCase):
    """Test unauthenticated Columna API access"""

    def setUp(self):
        self.client = APIClient()

    def test_auth_required(self):
        """Test that authentication is required"""
        res = self.client.get(SEMANA_URL)

        self.assertEqual(res.status_code, status.HTTP_401_UNAUTHORIZED)


class PrivateColumnaApiTests(TestCase):
    """Test unauthenticated Columna API access"""

    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            'testColumna@globalbit.co',
            'testpass'
        )
        self.client.force_authenticate(self.user)

    def test_retrieve_Columna(self):
        """Test retrieving a list of Columna"""
        sample_semanas(user=self.user)
        sample_semanas(user=self.user)

        res = self.client.get(SEMANA_URL)

        semana = Semana.objects.all().order_by('-id')
        serializer = SemanaSerializer(semana, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(res.data, serializer.data)


    def test_Columna_limited_to_user(self):
        """Test retrieving Columna for user"""
        user2 = get_user_model().objects.create_user(
            'othercolumna@globalbit.co',
            'password123'
        )
        sample_semanas( user=user2)
        sample_semanas(user=self.user)

        res = self.client.get(SEMANA_URL)

        semana = Semana.objects.order_by('-id')
        serializer = SemanaSerializer(semana, many=True)
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


    def test_create_Columna_with_Normalizacion(self):
        """Test parambd a semanas with Normalizacion"""
        normalizacion = sample_normalizacion(user=self.user)


        payload = {
            'normalizacion': normalizacion.id,
            'periodo' : '202010',
            'semanas' : 5,
        }
        res = self.client.post(SEMANA_URL, payload)

        self.assertEqual(res.status_code, status.HTTP_201_CREATED)
        semana = Semana.objects.get(id=res.data['id'])
        nombre_fuente = normalizacion.semanas_esp
        self.assertEqual(len(res.data), 4)
        self.assertEqual(semana.normalizacion.semanas_esp, nombre_fuente)


    def test_updatee_Columna_with_normalizacion(self):

        semana = sample_semanas(user=self.user)
        semana.normalizacion = sample_normalizacion(user=self.user)

        new_normalizacion = sample_normalizacion(user=self.user)

        payload = { 'normalizacion': new_normalizacion.id, 'semanas':2}
        url = detail_url(semana.id)
        
        self.client.patch(url, payload)

        semana.refresh_from_db()
        self.assertEqual(semana.semanas, payload['semanas'])

