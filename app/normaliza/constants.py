STATUS_CREATED = 'created'
STATUS_EXTRACT_RUNNING = 'extract-running'
STATUS_EXTRACT_SUCCESS = 'extract-done'
STATUS_EXTRACT_FAILURE = 'extract-failure'
STATUS_API_SUCCESS = 'api-done'
STATUS_API_FAILURE = 'api-failure'
STATUS_API_RUNNING = 'api-running'

SCHEDULED_TASK_ACTIVE = 'active'
SCHEDULED_TASK_PAUSED = 'paused'
SCHEDULED_TASK_FINISHED = 'finished'
SCHEDULED_TASKS_STATUS_CHOICES = (
    (SCHEDULED_TASK_ACTIVE, 'Activa'),
    (SCHEDULED_TASK_PAUSED, 'Pausada'),
    (SCHEDULED_TASK_FINISHED, 'Finalizada')
)

MINUTES_CHOICES = (
    ('Por minutos', [('*', 'Todos los minutos')] + [(f'{x}', f'{x}') for x in range(0, 60)]),
    ('Por intervalos', (
        ('*/5', 'Cada  minutos'),
        ('*/10', 'Cada 10 minutos'),
        ('*/15', 'Cada 15 minutos'),
        ('*/20', 'Cada 20 minutos'),
        ('*/25', 'Cada 25 minutos'),
        ('*/30', 'Cada 30 minutos')
    ))
)
HOURS_CHOICES = (
    ('Por horas', [('*', 'Todas las horas')] + [(f'{x}', f'{x}') for x in range(0, 24)]),
    ('Por intervalos', (
        ('*/2', 'Cada 2 horas'),
        ('*/4', 'Cada 4 horas'),
        ('*/6', 'Cada 6 horas'),
        ('*/8', 'Cada 8 horas'),
    ))
)
MONTHS_CHOICES = (
    ('*', 'Todos los meses'),
    ('0', 'Enero'),
    ('1', 'Febrero'),
    ('2', 'Marzo'),
    ('3', 'Abril'),
    ('4', 'Mayo'),
    ('5', 'Junio'),
    ('6', 'Julio'),
    ('7', 'Agosto'),
    ('8', 'Septiembre'),
    ('9', 'Octubre'),
    ('10', 'Noviembre'),
    ('11', 'Diciembre'),
)
MONTH_DAYS_CHOICES = (
    ('Por días', [('*', 'Todos los días del mes')] + [(f'{x}', f'{x}') for x in range(1, 32)]),
    ('Por intervalos', (
        ('*/2', 'Cada 2 días'),
        ('*/4', 'Cada 4 días'),
        ('*/6', 'Cada 6 días'),
        ('*/8', 'Cada 8 días'),
        ('*/10', 'Cada 10 días'),
        ('*/15', 'Cada 15 días'),
    ))
)

DAYS_OF_WEEK_CHOICES = (
    ('*', 'Todos los días de la semana'),
    ('1', 'Lunes'),
    ('2', 'Martes'),
    ('3', 'Miércoles'),
    ('4', 'Jueves'),
    ('5', 'Viernes'),
    ('6', 'Sábado'),
    ('0', 'Domingo'),
)
