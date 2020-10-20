import tempfile
import os


from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from rest_framework import status
from rest_framework.test import APIClient

from normaliza.models import ApiCatalogo, TipoFuente, ParametrosNorm, ParametrosBD

from normaliza.serializers import ParamBDSerializer, ParamBDDetailSerializer


PARAMETROSBD_URL = reverse('normaliza:parametrosbd-list')

def sample_apicatalogo(nombre='Normalizando'):
    return ApiCatalogo.objects.create(nombre=nombre)

def sample_tipofuente(nombre=3):
    return TipoFuente.objects.create(nombre=nombre)


def detail_url(paramtrosnorm_id):
    """Return recipe detail URL"""
    return reverse('normaliza:parametrosbd-detail', args=[paramtrosnorm_id])




def sample_paramnorm(user, **params):
    """Create and return a sample recipe"""
    defaults = {
        'ApiCatalogo': sample_apicatalogo(),
        'Fuente': sample_tipofuente(),
        'nombre': 'archivo1.txt',


    }
    defaults.update(params)

    return ParametrosNorm.objects.create(user=user, **defaults)



def sample_paramdb(user, **params):
    """Create and return a sample recipe"""
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




class PublicParamBDApiTests(TestCase):
    """Test unauthenticated recipe API access"""

    def setUp(self):
        self.client = APIClient()

    def test_auth_required(self):
        """Test that authentication is required"""
        res = self.client.get(PARAMETROSBD_URL)

        self.assertEqual(res.status_code, status.HTTP_401_UNAUTHORIZED)


class PrivateParamBDApiTests(TestCase):
    """Test unauthenticated recipe API access"""

    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            'test1@globalbit.co',
            'testpass'
        )
        self.client.force_authenticate(self.user)

    def test_retrieve_paramnorm(self):
        """Test retrieving a list of paramnorm"""
        sample_paramdb(user=self.user)
        sample_paramdb(user=self.user)

        res = self.client.get(PARAMETROSBD_URL)

        parambd = ParametrosBD.objects.all().order_by('-id')
        serializer = ParamBDSerializer(parambd, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(res.data, serializer.data)


    def test_paramnorm_limited_to_user(self):
        """Test retrieving paramnorm for user"""
        user2 = get_user_model().objects.create_user(
            'other@globalbit.co',
            'password123'
        )
        sample_paramdb(user=user2)
        sample_paramdb(user=self.user)

        res = self.client.get(PARAMETROSBD_URL)

        parambd = ParametrosBD.objects.all().order_by('-id')
        serializer = ParamBDSerializer(parambd, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(len(res.data), 2)
        self.assertEqual(res.data, serializer.data)


    def \
            test_view_parambd_detail(self):
        
        paramdb = sample_paramdb(user=self.user)
        paramdb.puerto= '1234'
        paramdb.host= 'localtest'
        paramdb.parametrosnorm=sample_paramnorm(user=self.user)


        url = detail_url(paramdb.id)
        res = self.client.get(url)

        serializer = ParamBDDetailSerializer(paramdb)
        #self.assertEqual(res.data, serializer.data)



    def test_create_parambd_with_paramnorm(self):
        """Test parambd a recipe with paramnorm"""
        paramnorm = sample_paramnorm(user=self.user)


        payload = {
        'parametrosnorm': paramnorm.id,
        'protocolo': 'tcp',
        'host': 'testglobalbit.com',
        'puerto': '3096',
        'SID': 'test',
        'ruta': 'Archivo.xlsx',
        'usuario': 'userdbtest',
        'password' : 'pruebas123',
        }
        res = self.client.post(PARAMETROSBD_URL, payload)

        self.assertEqual(res.status_code, status.HTTP_201_CREATED)
        paramBD = ParametrosBD.objects.get(id=res.data['id'])
        parametrosnorm = paramBD.parametrosnorm
        self.assertEqual(len(res.data), 9)
        self.assertEqual(paramnorm, parametrosnorm)


    def test_updatee_parambd_with_paramnomr(self):
        """test_updata parambd with paranorm"""
        paramdb = sample_paramdb(user=self.user)
        paramdb.parametrosnorm = sample_paramnorm(user=self.user)

        new_paramnorm = sample_paramnorm(user=self.user)

        payload = { 'parametrosnorm': new_paramnorm.id, 'puerto':'2323'}
        url = detail_url(paramdb.id)
        
        self.client.patch(url, payload)

        paramdb.refresh_from_db()
        self.assertEqual(paramdb.puerto, payload['puerto'])

