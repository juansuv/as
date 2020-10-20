from django.urls import path, include
from rest_framework.routers import DefaultRouter

from normaliza import views

router = DefaultRouter()
router.register('tipofuentes', views.TipoFuenteViewSet)
router.register('apicatalogos', views.ApiCatalogoViewSet)
router.register('apiusuarioparametros', views.ApiUsuarioParamViewSet)
router.register('parametrosnorms', views.ParamNormViewSet)
router.register('parametrosbds', views.ParamBDViewSet)
router.register('normalizacion', views.NormalizacionViewSet)
router.register('columnas', views.ColumnasViewSet)
router.register('filtros', views.FiltroViewSet)
router.register('semanas', views.SemanasViewSet)

app_name = 'normaliza'

urlpatterns = [
    path('', include(router.urls))
]
