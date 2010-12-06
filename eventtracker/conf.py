"""
Default settings for django-event-tracker. You can override any of these by
specifying EVENTS_<CONF_OPTION> in your projects' setting.py.
"""
from django.utils.importlib import import_module
from django.conf import settings as django_settings

class Settings(object):
    "Simple wrapper around config."
    def __init__(self, module_name, prefix=''):
        self.module = import_module(module_name)
        self.prefix = prefix

    def __getattr__(self, name):
        p_name = '_'.join((self.prefix, name))
        if hasattr(django_settings, p_name):
            return getattr(django_settings, p_name)
        return getattr(self.module, name)

    def __dir__(self):
        return dir(self.module)

MONGODB_HOSTS = [
#    'localhost:27017'
]

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017

RIGHT_MONGODB_HOST = None
RIGHT_MONGODB_PORT = 27017

# name of the db and collection in mongo
MONGODB_DB = 'events'
MONGODB_COLLECTION = 'events'

# messaging configuration
ROUTING_KEY = 'celery'
EXCHANGE = 'celery'
QUEUE = 'events'

# default backend
TRACKER_BACKEND = 'celery'

# time delta from (in days), that is used for dumping and delete data
EVENTTRACKING_RETENTION_INTERVAL = 30

# output file for dumping data from mongo db
MONGO_DUMP_FILE = 'mongo_dump_%s_%s_%s'


settings = Settings(__name__, 'EVENTS')
