import uuid
import os
from django.db import models
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, \
                                        PermissionsMixin
from django.conf import settings

from .constants import STATUS_CREATED, STATUS_EXTRACT_RUNNING, STATUS_EXTRACT_SUCCESS, \
    STATUS_EXTRACT_FAILURE, STATUS_API_SUCCESS, STATUS_API_FAILURE, STATUS_API_RUNNING, \
    MONTHS_CHOICES, MINUTES_CHOICES, HOURS_CHOICES, DAYS_OF_WEEK_CHOICES, MONTH_DAYS_CHOICES, \
    SCHEDULED_TASKS_STATUS_CHOICES, SCHEDULED_TASK_ACTIVE
API_NAME = 'generic'


def recipe_image_file_path(instance, filename):
    """Generate file path for new recipe image"""
    ext = filename.split('.')[-1]
    filename = f'{uuid.uuid4()}.{ext}'

    return os.path.join('uploads/recipe/', filename)


class UserManager(BaseUserManager):

    def create_user(self, email, password=None, **extra_fields):
        """Creates and saves a new user"""
        if not email:
            raise ValueError('Users must have an email address')
        user = self.model(email=self.normalize_email(email), **extra_fields)
        user.set_password(password)
        user.save(using=self._db)

        return user

    def create_superuser(self, email, password):
        """Creates and saves a new super user"""
        user = self.create_user(email, password)
        user.is_staff = True
        user.is_superuser = True
        user.save(using=self._db)

        return user


class User(AbstractBaseUser, PermissionsMixin):
    """Custom user model that suppors using email instead of username"""
    email = models.EmailField(max_length=255, unique=True)
    name = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)

    objects = UserManager()

    USERNAME_FIELD = 'email'

    def __str__(self):
        return self.name






class GenericExecution(models.Model):
    STATUS_CHOICES = (
        (STATUS_CREATED, 'Creado'),
        (STATUS_EXTRACT_RUNNING, 'extracion en ejecución'),
        (STATUS_EXTRACT_SUCCESS, 'extracion completado'),
        (STATUS_EXTRACT_FAILURE, 'extracion fallido'),
        (STATUS_API_RUNNING, 'api en ejecución'),
        (STATUS_API_SUCCESS, 'api completado'),
        (STATUS_API_FAILURE, 'api fallido'),
    )

    api_name = API_NAME
    name = models.CharField(max_length=100, verbose_name='Nombre')
    run_extract = models.BooleanField(verbose_name='Ejecutar extracion', default=False)
    run_transform = models.BooleanField(verbose_name='Ejecutar api', default=False)
    status = models.CharField(verbose_name='Estado', max_length=30, choices=STATUS_CHOICES, default=STATUS_CREATED)

    class Meta:
        # abstract = True
        verbose_name = 'Ejecución'
        verbose_name_plural = 'Ejecuciones'