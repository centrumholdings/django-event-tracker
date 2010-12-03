from datetime import datetime

from pymongo.connection import Connection

from django.db import models
from pymongo import Connection

from eventtracker.conf import settings

def get_mongo_collection():
    "Open a connection to MongoDB and return the collection to use."
    if not getattr(settings, "MONGODB_HOSTS", []):
        hosts = [
            "%s:%s" % (getattr(settings, "MONGODB_HOST", "localhost"), getattr(settings, "MONGODB_PORT", 27017))
        ]

        if getattr(settings, "RIGHT_MONGODB_HOST", None):
            hosts.append("%s:%s" % (settings.RIGHT_MONGODB_HOST, getattr(settings, "RIGHT_MONGODB_PORT", 27017)))
    else:
        hosts = settings.MONGODB_HOSTS
    
    connection = Connection(hosts)
    return connection[settings.MONGODB_DB][settings.MONGODB_COLLECTION]
    
def save_event(collection, event, timestamp, params):
    "Save the event in MongoDB collection"
    collection.insert({
        'event': event,
        'timestamp': datetime.fromtimestamp(timestamp),
        'params': params
    }) 

class Event(models.Model):
    "Dummy model for development."
    timestamp = models.DateTimeField(auto_now_add=True)
    event = models.SlugField()
    params = models.TextField()
