import anyjson

from django.http import HttpResponse, HttpResponseBadRequest

from eventtrackerrt import backend
#new:
#from eventtrackerrt import tasks

def track_event(request, event):
    """
    Simple view that receives and stores an event in the backend (celery queue
    or dummy).
    """
    params = request.GET.get('params')
    if params:
        try:
            params = anyjson.deserialize(params)
        except ValueError, e:
            return HttpResponseBadRequest()
    else:
        params = {}

    backend.track(event, params)
    #new:
    #if settings.TRACKER_BACKEND == 'celery':
#	tasks.take_event.delay()
    return HttpResponse('OK')

