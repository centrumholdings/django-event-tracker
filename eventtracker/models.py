from datetime import datetime

from django.db import models

from eventtracker.conf import settings

def get_mongo_collection():
    "Open a connection to MongoDB and return the collection to use."
    connection = Connection(settings.MONGODB_HOSTS)
    return connection[settings.MONGODB_DB][settings.MONGODB_COLLECTION]
    

def save_event(collection, event, timestamp, params):
    collection.insert({
        'event': event,
        'timestamp': datetime.fromtimestamp(timestamp),
        'params': params
    }) 

class Event(models.Model):
    timestamp = models.DateTimeField(auto_now_add=True)
    event = models.SlugField()
    params = models.TextField()
