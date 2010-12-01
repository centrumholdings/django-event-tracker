====================
Django event tracker
====================

Django-event-tracker is a simple application that enables event tracking via a
simple HTTP GET request.

Installation
============

 * install django event tracker into your system or ``virtualenv``
 * `configure celery`_ to work with your project
 * add ``eventtracker`` to your ``INSTALLED_APPS`` and ``eventtracker.urls``
   somewhere in your URLs
 * customize your settings, see ``eventtracker.conf`` for complete list of
   options and their default values

.. _`configure celery`: http://celeryproject.org/introduction.html#configuring-your-django-project-to-use-celery

Use
===

To store an event, do a GET request on ``eventtracker.views.track_event``. The
parameter in the URL mapping will determine the name of the event (only
required attribute) and you can supply additional parameters by passing in a
json object as GET parameter ``params``.

Depending on your ``EVENTS_TRACKER_BACKEND`` settings the event will either be
directly stored in the django database (value ``'dummy'``, do not use in
production) or sent into a queue using ``carrot``. The data will be collected
by celeryd once every three minutes (by default) and stored into MongoDB.

Querying and analyzing of the data is left entirely to MongoDB since it is very
good at that.

For dumping and removal data from mongo was added two management commands: dumprecords and deleterecords.
In settings is set constant EVENTTRACKING_RETENTION_INTERVAL (default 30 days) as time delta for dumping and removal.
Use dumprecords with argument output file and two optional arguments (date now in format dd-MM-YYYY and time delta)
Use deleterecords with two optional arguments (date now in format dd-MM-YYYY and time delta) 


