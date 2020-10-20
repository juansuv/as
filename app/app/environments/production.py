import os

from django.utils.log import DEFAULT_LOGGING

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DEBUG = False

SITE_URL = 'http://clo819.tecnoquimicas.com'

ALLOWED_HOSTS = ['clo819.tecnoquimicas.com']

DATABASES = {
    'default': {
        'ENGINE': 'sql_server.pyodbc',
        'NAME': 'dbDataLab',
        'USER': 'normapl',
        'PASSWORD': 'iAmEkenXE2PsLG4',
        'HOST': 'dev658.tecnoquimicas.com',
        'OPTIONS': {
            'driver': 'ODBC Driver 17 for SQL Server'
        },
    }
}

MEDIA_ROOT = os.path.join(os.sep, 'apl', 'normalizacion')

LOGGING = DEFAULT_LOGGING

EMAIL_HOST = 'smtp.mandrillapp.com'
EMAIL_HOST_USER = 'soporte@globalbit.co'
EMAIL_HOST_PASSWORD = '5VS5_kTP9LF6VS7Hn5vP0Q'
EMAIL_PORT = 587
EMAIL_USE_TLS = True
DEFAULT_FROM_EMAIL = 'Notificaciones NORMALIZACION <soporte@globalbit.co>'
BCC_EMAILS = []
EMAIL_RECIPIENTS = []

AUTH_WSDL_PATH = os.path.join(_BASE_DIR, 'eds', 'auth_wsdls/development.wsdl')
