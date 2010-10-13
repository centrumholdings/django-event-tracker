from django.conf.urls.defaults import *
from eventtrackerrt import views

urlpatterns = patterns('',
    url(r'^(?P<event>[\w-]+)/$', views.track_event, name='eventtrackerrt-track-event'),
)
