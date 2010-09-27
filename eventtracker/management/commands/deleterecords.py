"""
This file delete records from mongo, that are elderly then date argument (first parametr), second parametr is date now
"""
import sys
import pymongo
from datetime import datetime, timedelta

from django.core.management.base import BaseCommand, CommandError

from eventtracker.conf import settings



class Command(BaseCommand):
    args = '<timedelta(days) date_now(dd-MM-YYYY) ...>'
    help = 'delete data from the file by refer first argument (date from that delete data in days)'

    def handle(self, *args, **options):
	"""
	delete data from the file by refer argument (date from that delete data)   
	"""
	
	date_arg = args[1].split("-")
	now_date = datetime(int(date_arg[2]), int(date_arg[1]), int(date_arg[0]))
	try:
	    date = now_date - timedelta( days=int(args[0]) )
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
