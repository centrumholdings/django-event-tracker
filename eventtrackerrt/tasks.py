from datetime import timedelta, datetime
from time import time
#new:
#from celery.decorators import task
#endnew
from celery.task import Task
from celery.registry import tasks
from carrot.connection import DjangoBrokerConnection
from carrot.messaging import Publisher, Consumer

from eventtrackerrt.conf import settings
from eventtrackerrt import models 

publisher = None

def _get_carrot_object(klass, **kwargs):
    "Helper function to create Publisher and Consumer objects."
    return klass(
            connection=DjangoBrokerConnection(),
            exchange=settings.EXCHANGE,
            routing_key=settings.ROUTING_KEY,
            exchange_type="topic",
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
    occurs during sending of the message, close the Publisher so it will be
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
        # put the message into the queue including current time
        publisher.send((event, time(), params))
	print "me-do it"
	
    except:
        # something went wrong, probably a connection error or something. Close
        # the carrot connection and set it to None so that the next request
        # will try and reopen it.
        _close_carrot_object(publisher)
        publisher = None
        raise


def collect_events():
    """
    Collect all events waiting in the queue and store them in the database.
    """
    consumer = None
    collection = None
    try:
        consumer = _get_carrot_object(Consumer, queue=settings.QUEUE)
        collection = models.get_mongo_collection()
	print "I work"
        for message in consumer.iterqueue():
            e, t, p = message.decode()
            models.save_event(collection, e, t, p)
            message.ack()

    finally:
        _close_carrot_object(consumer)
        if collection:
            try:
                collection.connection.close()
            except:
                pass
   

class ProcessEventTask(Task):
    "Celery task that collect event from queue."
    name="eventtrackerrt.tasks.ProcessEventTask"

    def run(self, **kwargs):
	collect_events()
	

tasks.register(ProcessEventTask)