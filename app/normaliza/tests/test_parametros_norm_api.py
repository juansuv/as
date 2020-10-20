import tempfile
import os


from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from rest_framework import status
from rest_framework.test import APIClient

from normaliza.models import ApiCatalogo, TipoFuente, ParametrosNorm

from normaliza.serializers import ParamNormSerializer


PARAMETROSNORM_URL = reverse('normaliza:parametrosnorm-list')

def sample_apicatalogo(nombre='Normalizando'):
    return ApiCatalogo.objects.create(nombre=nombre)

def sample_tipofuente(nombre=1):
    return TipoFuente.objects.create(nombre=nombre)


def detail_url(paramtrosnorm_id):
    """Return recipe detail URL"""
    return reverse('normaliza:parametrosnorm-detail', args=[paramtrosnorm_id])




def sample_paramnorm(user, **params):
    """Create and return a sample recipe"""
    defaults = {
        'ApiCatalogo': sample_apicatalogo(),
        'Fuente': sample_tipofuente(),
        'nombre': 'archivo1.txt',


    }
    defaults.update(params)

    return ParametrosNorm.objects.create(user=user, **defaults)


class PublicParamNormApiTests(TestCase):
    """Test unauthenticated recipe API access"""

    def setUp(self):
        self.client = APIClient()

    def test_auth_required(self):
        """Test that authentication is required"""
        res = self.client.get(PARAMETROSNORM_URL)

        self.assertEqual(res.status_code, status.HTTP_401_UNAUTHORIZED)


class PrivateParamNormApiTests(TestCase):
    """Test unauthenticated recipe API access"""

    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            'test2@globalbit.co',
            'testpass'
        )
        self.client.force_authenticate(self.user)

    def test_retrieve_paramnorm(self):
        """Test retrieving a list of paramnorm"""
        sample_paramnorm(user=self.user)
        sample_paramnorm(user=self.user)

        res = self.client.get(PARAMETROSNORM_URL)

        paramnorm = ParametrosNorm.objects.all().order_by('-id')
        serializer = ParamNormSerializer(paramnorm, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(res.data, serializer.data)


    def test_paramnorm_limited_to_user(self):
        """Test retrieving paramnorm for user"""
        user2 = get_user_model().objects.create_user(
            'other@globalbit.co',
            'password123'
        )
        sample_paramnorm(user=user2)
        sample_paramnorm(user=self.user)

        res = self.client.get(PARAMETROSNORM_URL)

        paramnorm = ParametrosNorm.objects.order_by('-id')
        serializer = ParamNormSerializer(paramnorm, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(len(res.data), 2)
        self.assertEqual(res.data, serializer.data)

    def test_create_paramnorm_with_apicatalogo(self):
        """Test paramnorm a recipe with apicatalogo y tipofuente"""
        apicatalogo1 = sample_apicatalogo()
        tipofuente = sample_tipofuente()

        payload = {
            'ApiCatalogo':  apicatalogo1.id,
            'Fuente': tipofuente.id,
            'nombre': 'prueba.xlsx'
        }
        res = self.client.post(PARAMETROSNORM_URL, payload)

        self.assertEqual(res.status_code, status.HTTP_201_CREATED)
        paramnorm = ParametrosNorm.objects.get(id=res.data['id'])
        apicatalogo = paramnorm.ApiCatalogo
        self.assertEqual(len(res.data), 5)
        self.assertEqual(apicatalogo1, apicatalogo)

'''
    def test_updatee_api_user_with_apicactologo(self):

        apiuser = sample_paramnorm(user=self.user)
        apiuser.ApiCatalogo = sample_apicatalogo(nombre="prueba")
        new_apicatalogo = sample_apicatalogo(nombre='Normalizando')

        payload = { 'ApiCatalogo': new_apicatalogo}
        url = detail_url(apiuser.id)
        
        self.client.patch(url, payload)

        apiuser.refresh_from_db()
        self.assertEqual(apiuser.ApiCatalogo.nombre, payload['ApiCatalogo'].nombre)

'''