from django.contrib.auth import get_user_model
from django.urls import reverse
from django.test import TestCase

from rest_framework import status
from rest_framework.test import APIClient

from normaliza.models import TipoFuente

from normaliza.serializers import TipoFuenteSerializer


TIPOFUENTES_URL = reverse('normaliza:tipofuente-list')


class PublicTipoFuentesApiTests(TestCase):
    """Test si el tipo de fuentes es publico API"""

    def setUp(self):
        self.client = APIClient()

    def test_login_required(self):
        """Test que mira si el login es requerido para obtener los tipo de fuente"""
        res = self.client.get(TIPOFUENTES_URL)

        self.assertEqual(res.status_code, status.HTTP_401_UNAUTHORIZED)


class PrivateTipoFuentesApiTests(TestCase):
    """Test para usuarios autorizados API"""

    def setUp(self):
        self.user = get_user_model().objects.create_user(
            'juan.suarez@globalbit.co',
            'estudie123'
        )
        self.client = APIClient()
        self.client.force_authenticate(self.user)

    def test_retrieve_tipofuentes(self):
        """Test para recibir los tipos de usuarios"""
        TipoFuente.objects.create(nombre=1)
        TipoFuente.objects.create(nombre=2)
        TipoFuente.objects.create(nombre=3)
        TipoFuente.objects.create(nombre=4)

        res = self.client.get(TIPOFUENTES_URL)

        tipofuentes = TipoFuente.objects.all()
        serializer = TipoFuenteSerializer(tipofuentes, many=True)
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(res.data, serializer.data)

    def test_tipofuente_limited_to_user(self):
        """Test that tags returned are for the authenticated user"""
        user2 = get_user_model().objects.create_user(
            'other@globalbit.co',
            'testpass'
        )
        TipoFuente.objects.create(nombre=2)
        tipofuente = TipoFuente.objects.create(nombre=1)

        res = self.client.get(TIPOFUENTES_URL)

        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(len(res.data), 4)
        self.assertEqual(res.data[0]['nombre'], 1)


    def test_create_tipo_fuente_successful(self):
        """Test creando un nuevo tipo de fuente"""
        payload = {'nombre': 1}
        self.client.post(TIPOFUENTES_URL, payload)
        exists = TipoFuente.objects.filter(

            nombre=payload['nombre']
        ).exists()
        self.assertTrue(exists)

    def test_create_tag_invalid(self):
        """Test creando un nuevo tipo de fuente con datos erroneos"""
        payload = {'nombre': 'sa'}
        res = self.client.post(TIPOFUENTES_URL, payload)

        self.assertEqual(res.status_code, status.HTTP_400_BAD_REQUEST)
