from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework import viewsets, mixins, status
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated
from django.core.files.storage import FileSystemStorage
from normaliza.data_flow import DataFlow

from normaliza.models import TipoFuente, ApiCatalogo, ApiUsuarioParametro, \
    ParametrosNorm, ParametrosBD, Normalizacion, Columna, Filtro, Semana


from normaliza import serializers
from django.shortcuts import render


# Create your views here.
class BaseRecipeAttrViewSet(viewsets.GenericViewSet,
                            mixins.ListModelMixin,
                            mixins.CreateModelMixin):
    """Base viewset for user owned recipe attributes"""
    authentication_classes = (TokenAuthentication,)
    permission_classes = (IsAuthenticated,)

    def get_queryset(self):
        """Return objects for the current authenticated user only"""
        assigned_only = bool(
            int(self.request.query_params.get('assigned_only', 0))
        )
        queryset = self.queryset

        return queryset

    def perform_create(self, serializer):
        """Create a new object"""
        serializer.save()


class TipoFuenteViewSet(BaseRecipeAttrViewSet):
    """Manage tipo fuente en la base de datos"""
    queryset = TipoFuente.objects.all()

    serializer_class = serializers.TipoFuenteSerializer
    authentication_classes = (TokenAuthentication,)
    permission_classes = (IsAuthenticated,)


class ApiCatalogoViewSet(BaseRecipeAttrViewSet):
    """Manage ApiCatalogo in the database"""

    queryset = ApiCatalogo.objects.all().order_by('-nombre').distinct()
    serializer_class = serializers.ApiCatalogoSerializer
    authentication_classes = (TokenAuthentication,)
    permission_classes = (IsAuthenticated,)


class ApiUsuarioParamViewSet(viewsets.ModelViewSet):
    """Manage ApiUsuarioParametro in the database"""
    serializer_class = serializers.ApiUsuarioParamSerializer
    queryset = ApiUsuarioParametro.objects.all()
    authentication_classes = (TokenAuthentication,)
    permission_classes = (IsAuthenticated,)

    def _params_to_ints(self, qs):
        """Convert a list of string IDs to a list of integers"""
        return [int(str_id) for str_id in qs.split(',')]

    def get_queryset(self):
        """Retrieve the ApiUsuarioParametro for the authenticated user"""
        queryset = self.queryset.order_by('-id')

        return queryset

    def get_serializer_class(self):
        """Return appropriate serializer class"""

        if self.action == 'retrieve':
            return serializers.ApiUsuarioParamDetailSerializer
        '''
        elif self.action == 'upload_image':
            return serializers.RecipeImageSerializer
        '''
        return self.serializer_class

    def perform_create(self, serializer):
        """Create a new ApiUsuarioParametro"""
        serializer.save(user=self.request.user)


class ParamNormViewSet(viewsets.ModelViewSet):
    """Manage ParametrosNorm in the database"""
    serializer_class = serializers.ParamNormSerializer
    queryset = ParametrosNorm.objects.all()
    authentication_classes = (TokenAuthentication,)
    permission_classes = (IsAuthenticated,)

    def _params_to_ints(self, qs):
        """Convert a list of string IDs to a list of integers"""
        return [int(str_id) for str_id in qs.split(',')]

    def get_queryset(self):
        """Retrieve the recipes for the authenticated user"""
        queryset = self.queryset.order_by('-id')

        return queryset

    def get_serializer_class(self):
        """Return appropriate serializer class"""

        if self.action == 'retrieve':
            return serializers.ParamNormDetailSerializer
        '''
        elif self.action == 'upload_image':
            return serializers.RecipeImageSerializer
        '''
        return self.serializer_class

    def perform_create(self, serializer):
        """Create a new recipe"""
        serializer.save(user=self.request.user)


class ParamBDViewSet(viewsets.ModelViewSet):
    """Manage ParametrosBD in the database"""
    serializer_class = serializers.ParamBDSerializer
    queryset = ParametrosBD.objects.all()
    authentication_classes = (TokenAuthentication,)
    permission_classes = (IsAuthenticated,)

    def _params_to_ints(self, qs):
        """Convert a list of string IDs to a list of integers"""
        return [int(str_id) for str_id in qs.split(',')]

    def get_queryset(self):
        """Retrieve the normalizacion bd for the authenticated user"""
        queryset = self.queryset.order_by('-id')
        for query in queryset:
            self.datos = query
        return queryset

    def get_serializer_class(self):
        """Return appropriate serializer class"""

        if self.action == 'retrieve':
            return serializers.ParamBDDetailSerializer

        return self.serializer_class

    def perform_create(self, serializer):
        """Create a new parambd"""

        serializer.save(user=self.request.user)


class NormalizacionViewSet(viewsets.ModelViewSet):
    """Manage Normalizacion in the database"""
    serializer_class = serializers.NormalizacionSerializer
    queryset = Normalizacion.objects.all()
    datos = None

    authentication_classes = (TokenAuthentication,)
    permission_classes = (IsAuthenticated,)

    def _params_to_ints(self, qs):
        """Convert a list of string IDs to a list of integers"""
        return [int(str_id) for str_id in qs.split(',')]

    def get_queryset(self):
        """Retrieve the normalizacion bd for the authenticated user"""
        queryset = self.queryset.order_by('-id')
        for query in queryset:
            self.datos = query
        return queryset

    def get_serializer_class(self):
        """Return appropriate serializer class"""

        if self.action == 'retrieve':
            return serializers.NormalizacionDetailSerializer
        elif self.action == 'create':
            return serializers.NormalizacionPostSerializer

        return self.serializer_class

    def perform_create(self, serializer):
        """Create a new parambd"""
        # contruir columas
        '''
        for query in self.request:
            api_param = query.get_api_param()
            
            self.datos = query.get_columnas()
            
            
            
            
        '''

        data = serializer.validated_data.copy()
        data_flow = DataFlow()
        uuid = data_flow.normalize(data, user=self.request.user)

        serializer.validated_data['uuid'] = uuid

        serializer.save(user=self.request.user)


class ColumnasViewSet(viewsets.ModelViewSet):
    """Manage Columna in the database"""
    serializer_class = serializers.ColumnaSerializer
    queryset = Columna.objects.all()
    authentication_classes = (TokenAuthentication,)
    permission_classes = (IsAuthenticated,)

    def _params_to_ints(self, qs):
        """Convert a list of string IDs to a list of integers"""
        return [int(str_id) for str_id in qs.split(',')]

    def get_queryset(self):
        """Retrieve the Columna for the authenticated user"""
        queryset = self.queryset.order_by('-id')

        return queryset

    def get_serializer_class(self):
        """Return appropriate serializer class"""

        if self.action == 'retrieve':
            return serializers.ColumnaDetailSerializer

        return self.serializer_class

    def perform_create(self, serializer):
        """Create a new Columna"""
        serializer.save(user=self.request.user)


class FiltroViewSet(viewsets.ModelViewSet):
    """Manage Columna in the database"""
    serializer_class = serializers.FiltroSerializer
    queryset = Filtro.objects.all()
    authentication_classes = (TokenAuthentication,)
    permission_classes = (IsAuthenticated,)

    def _params_to_ints(self, qs):
        """Convert a list of string IDs to a list of integers"""
        return [int(str_id) for str_id in qs.split(',')]

    def get_queryset(self):
        """Retrieve the Columna for the authenticated user"""
        queryset = self.queryset.order_by('-id')

        return queryset

    def get_serializer_class(self):
        """Return appropriate serializer class"""

        if self.action == 'retrieve':
            return serializers.FiltroDetailSerializers

        return self.serializer_class

    def perform_create(self, serializer):
        """Create a new Columna"""
        serializer.save(user=self.request.user)


class SemanasViewSet(viewsets.ModelViewSet):
    """Manage Columna in the database"""
    serializer_class = serializers.SemanaSerializer
    queryset = Semana.objects.all()
    authentication_classes = (TokenAuthentication,)
    permission_classes = (IsAuthenticated,)

    def _params_to_ints(self, qs):
        """Convert a list of string IDs to a list of integers"""
        return [int(str_id) for str_id in qs.split(',')]

    def get_queryset(self):
        """Retrieve the Columna for the authenticated user"""
        queryset = self.queryset.order_by('-id')

        return queryset

    def get_serializer_class(self):
        """Return appropriate serializer class"""

        if self.action == 'retrieve':
            return serializers.SemanaSerializer

        return self.serializer_class

    def perform_create(self, serializer):
        """Create a new Columna"""
        serializer.save()
