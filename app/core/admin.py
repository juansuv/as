from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.utils.translation import gettext as _

from core import models
from normaliza import models as models_normaliza

class UserAdmin(BaseUserAdmin):
    ordering = ['id']
    list_display = ['email', 'name']
    fieldsets = (
        (None, {'fields': ('email', 'password')}),
        (_('Personal Info'), {'fields': ('name',)}),
        (
            _('Permissions'),
            {'fields': ('is_active', 'is_staff', 'is_superuser')}
        ),
        (_('Important dates'), {'fields': ('last_login',)})
    )
    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('email', 'password1', 'password2')
        }),
    )


admin.site.register(models.User, UserAdmin)
admin.site.register(models_normaliza.TipoFuente)
admin.site.register(models_normaliza.ApiCatalogo)
admin.site.register(models_normaliza.ApiUsuarioParametro)
admin.site.register(models_normaliza.ParametrosNorm)
admin.site.register(models_normaliza.ParametrosBD)
admin.site.register(models_normaliza.Normalizacion)
admin.site.register(models_normaliza.Semana)
admin.site.register(models_normaliza.Filtro)
admin.site.register(models_normaliza.Columna)
