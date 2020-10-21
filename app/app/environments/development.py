import os

from django.utils.log import DEFAULT_LOGGING

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DEBUG = True

#SITE_URL = 'http://dev859.tecnoquimicas.com'
SITE_URL = 'localhost'

#ALLOWED_HOSTS = ['172.22.12.155', 'dev859.tecnoquimicas.com']
ALLOWED_HOSTS = ['127.0.0.1', 'localhost']
SECURE_SSL_REDIRECT = True

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

#MEDIA_ROOT = os.path.join(os.sep, 'apl', 'eds')

LOGGING = DEFAULT_LOGGING

EMAIL_HOST = 'smtp.mandrillapp.com'
EMAIL_HOST_USER = 'soporte@globalbit.co'
EMAIL_HOST_PASSWORD = '5VS5_kTP9LF6VS7Hn5vP0Q'
EMAIL_PORT = 587
EMAIL_USE_TLS = True
DEFAULT_FROM_EMAIL = 'Notificaciones EDS <soporte@globalbit.co>'
BCC_EMAILS = []
EMAIL_RECIPIENTS = []



