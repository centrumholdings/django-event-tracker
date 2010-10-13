from eventtrackerrt.conf import settings

__all__ = ['track']

if settings.TRACKER_BACKEND == 'celery':
    from eventtrackerrt.tasks import track
else:
    from eventtrackerrt.dummy import track
