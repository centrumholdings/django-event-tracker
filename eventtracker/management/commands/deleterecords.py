"""
This file delete records from mongo, that are elderly then date argument (first parametr), second parametr is date now
"""
import sys
import pymongo
from datetime import datetime, timedelta

from django.core.management.base import BaseCommand, CommandError

from eventtracker.conf import settings



class Command(BaseCommand):
    args = '<date_now(dd-MM-YYYY) timedelta(days) >'
    help = 'delete data from the file by refer first argument (date from that delete data in days)'

    def handle(self, *args, **options):
	"""
	delete data from the file by refer argument (date from that delete data)   
	"""
	
	try:
	    date_arg = args[0].split("-")
	    now_date = datetime(int(date_arg[2]), int(date_arg[1]), int(date_arg[0]))
	except IndexError:
	    now_date_full = datetime.now() - timedelta(days=1)
	    now_date = datetime(now_date_full.year, now_date_full.month, now_date_full.day, 0, 0, 0)
	
	try:
	    retention_interval = int(args[1])
	except IndexError:
	    retention_interval = settings.EVENTTRACKING_RETENTION_INTERVAL

	try:
	    date = now_date - timedelta( days=retention_interval )
	except TypeError:
	    raise CommandError('error - bad time argument')
	
	connection = pymongo.Connection()
	db = connection[settings.MONGODB_DB]
	collection = db[settings.MONGODB_COLLECTION]
    
	try:
	    collection.remove({"timestamp": {"$lt": date}})
	except:
	    raise CommandError('error - can not remove data')

	print 'Successfull: records removed'
