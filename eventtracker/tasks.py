from time import time

from celery.decorators import task
from carrot.connection import DjangoBrokerConnection
from carrot.messaging import Publisher, Consumer
from celery.utils import gen_unique_id

from eventtracker.conf import settings
from eventtracker import models 

publisher = None

def _get_carrot_object(klass, **kwargs):
    "Helper function to create Publisher and Consumer objects."
    return klass(
            connection=DjangoBrokerConnection(),
            exchange=settings.EXCHANGE,
            routing_key=settings.ROUTING_KEY,
            **kwargs
        )
    
def _close_carrot_object(carobj):
    "Close Consumer or Publisher safely."
    if carobj:
        try:
            carobj.close()
        except:
            pass
        try:
            carobj.connection.close()
        except:
            pass


def track(event, params):
    """
    Dispatch a track event request into the queue.

    If the Publisher object hasn't been intialized yet, do so. If any error
    occurs during sending of the task, close the Publisher so it will be
    open automatically the next time somedy tracks an event. This will prevent
    a short-term network failure to disable one thread from commucating with
    the queue at the cost of retrying the connection every time.
    """
    global publisher
    if publisher is None:
        # no connection or there was an error last time
        # reinitiate the Publisher
        publisher = _get_carrot_object(Publisher)

    try:
        # put the task into the queue 
	publisher.send({"task": "tasks.collect_events", "args": (event, time(), params), "kwargs": {}, "id": gen_unique_id()})
    except:
        # something went wrong, probably a connection error or something. Close
        # the carrot connection and set it to None so that the next request
        # will try and reopen it.
        _close_carrot_object(publisher)
        publisher = None
        raise


@task(name="tasks.collect_events")
def collect_events(e, t, p):
    """
    Collect task waiting in the queue and store event in the database.
    """
    collection = None
    try:
        collection = models.get_mongo_collection()
        models.save_event(collection, e, t, p)
	
    finally:
        if collection:
            try:
                collection.connection.close()
            except:
                pass
