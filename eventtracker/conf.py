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

ROUTING_KEY = 'events'
EXCHANGE = 'events'
QUEUE = 'events'
TASK_PERIOD = 3*60

TRACKER_BACKEND = 'celery'

settings = Settings(__name__, 'EVENTS_')
