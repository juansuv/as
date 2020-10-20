from django.contrib.auth import get_user_model
from django.urls import reverse
from django.test import TestCase

from rest_framework import status
from rest_framework.test import APIClient

from normaliza.models import ApiCatalogo

from normaliza.serializers import ApiCatalogoSerializer


APISCATALOGO_URL = reverse('normaliza:apicatalogo-list')


class PublicApiCatalogosApiTests(TestCase):
    """Test si el Api catalogo es publico API"""

    def setUp(self):
        self.client = APIClient()

    def test_login_required(self):
        """Test que mira si el login es requerido para obtener los tipo de fuente"""
        res = self.client.get(APISCATALOGO_URL)

        self.assertEqual(res.status_code, status.HTTP_401_UNAUTHORIZED)


class PrivateApiCatalogosApiTests(TestCase):
    """Test para usuarios autorizados API"""

    def setUp(self):
        self.user = get_user_model().objects.create_user(
            'juan.suarez@globalbit.co',
            'estudie123'
        )
        self.client = APIClient()
        self.client.force_authenticate(self.user)

    def test_retrieve_apicatalogos(self):
        """Test para recibir los tipos de usuarios"""
        ApiCatalogo.objects.create(nombre='normalizacion')
        ApiCatalogo.objects.create(nombre='factorizacion')
        ApiCatalogo.objects.create(nombre='proyeccion')
        ApiCatalogo.objects.create(nombre='normalizada1')


        res = self.client.get(APISCATALOGO_URL)

        apicatalogos = ApiCatalogo.objects.all().order_by('-nombre').distinct()
        serializer = ApiCatalogoSerializer(apicatalogos, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        #self.assertEqual(res.data, serializer.data)

    def test_apicatalogo_limited_to_user(self):
        """Test that tags returned are for the authenticated user"""
        user2 = get_user_model().objects.create_user(
            'other@globalbit.co',
            'testpass'
        )
        ApiCatalogo.objects.create(nombre='integracion')
        apicatalogo = ApiCatalogo.objects.create(nombre='normalizados')

        res = self.client.get(APISCATALOGO_URL)

        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(len(res.data), 2)
        self.assertEqual(res.data[0]['nombre'], 'normalizados')


    def test_create_tipo_fuente_successful(self):
        """Test creando un nueva api en el catalogo"""
        payload = {'nombre': 'Test normalizacion'}
        self.client.post(APISCATALOGO_URL, payload)
        exists = ApiCatalogo.objects.filter(

            nombre=payload['nombre']
        ).exists()
        self.assertTrue(exists)

    def test_create_tag_invalid(self):
        """Test creando un nueva api en el catalogo con datos erroneos"""
        payload = {'nombre': ''}
        res = self.client.post(APISCATALOGO_URL, payload)

        self.assertEqual(res.status_code, status.HTTP_400_BAD_REQUEST)
