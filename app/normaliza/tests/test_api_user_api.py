import tempfile
import os


from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from rest_framework import status
from rest_framework.test import APIClient

from normaliza.models import ApiCatalogo, ApiUsuarioParametro

from normaliza.serializers import ApiUsuarioParamSerializer


APIUSERPARAM_URL = reverse('normaliza:apiusuarioparametro-list')

def sample_apicatalogo(nombre='Normalizando'):
    return ApiCatalogo.objects.create(nombre=nombre)

def detail_url(apiuserparam_id):
    """Return recipe detail URL"""
    return reverse('normaliza:apiusuarioparametro-detail', args=[apiuserparam_id])




def sample_apiuserparam(user, **params):
    """Create and return a sample recipe"""
    defaults = {
        'Api_catalogo': sample_apicatalogo(),

    }
    defaults.update(params)

    return ApiUsuarioParametro.objects.create(user=user, **defaults)


class PublicApiUserParamApiTests(TestCase):
    """Test unauthenticated recipe API access"""

    def setUp(self):
        self.client = APIClient()

    def test_auth_required(self):
        """Test that authentication is required"""
        res = self.client.get(APIUSERPARAM_URL)

        self.assertEqual(res.status_code, status.HTTP_401_UNAUTHORIZED)


class PrivateApiUserParamApiTests(TestCase):
    """Test unauthenticated recipe API access"""

    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            'test3@globalbit.co',
            'testpass'
        )
        self.client.force_authenticate(self.user)

    def test_retrieve_apiuserparam(self):
        """Test retrieving a list of apiuserparam"""
        sample_apiuserparam(user=self.user)
        sample_apiuserparam(user=self.user)

        res = self.client.get(APIUSERPARAM_URL)

        apiuserparam = ApiUsuarioParametro.objects.all().order_by('-id')
        serializer = ApiUsuarioParamSerializer(apiuserparam, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(res.data, serializer.data)

    def test_apiuserparam_limited_to_user(self):
        """Test retrieving apiuserparam for user"""
        user2 = get_user_model().objects.create_user(
            'other@globalbit.co',
            'password123'
        )
        sample_apiuserparam(user=user2)
        sample_apiuserparam(user=self.user)

        res = self.client.get(APIUSERPARAM_URL)

        apiuserparam = ApiUsuarioParametro.objects.order_by('-id')
        serializer = ApiUsuarioParamSerializer(apiuserparam, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(len(res.data), 2)
        self.assertEqual(res.data, serializer.data)

    def test_create_apiuserparam_with_apicatalogo(self):
        """Test apiuserparam a recipe with apicatalogo"""
        apicatalogo1 = sample_apicatalogo()

        payload = {
            'Api_catalogo':  [apicatalogo1.id]
        }
        res = self.client.post(APIUSERPARAM_URL, payload)

        self.assertEqual(res.status_code, status.HTTP_201_CREATED)
        apiuserparam = ApiUsuarioParametro.objects.get(id=res.data['id'])
        apicatalogo = apiuserparam.Api_catalogo
        self.assertEqual(len(res.data), 2)
        self.assertEqual(apicatalogo1, apicatalogo)


    def test_updatee_api_user_with_apicactologo(self):

        apiuser = sample_apiuserparam(user=self.user)
        apiuser.Api_catalogo = sample_apicatalogo(nombre="prueba")
        new_apicatalogo = sample_apicatalogo(nombre='Normalizando')

        payload = { 'Api_catalogo': new_apicatalogo.id}
        url = detail_url(apiuser.id)
        
        self.client.patch(url, payload)

        apiuser.refresh_from_db()
        self.assertEqual(apiuser.Api_catalogo.id, payload['Api_catalogo'])

