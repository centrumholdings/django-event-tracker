"""
This file takes records from mongo, that are elderly then date argument (third parametr) and save it in file (first argument), 
second parametr is date now a third parametr is timedelta. Every parametrs are optional
"""
import sys
import string
from datetime import datetime, timedelta
from subprocess import Popen, PIPE

from django.core.management.base import BaseCommand, CommandError

from eventtracker.conf import settings



class Command(BaseCommand):
    args = '<output_file date_now(dd-MM-YYYY) timedelta(days)>'
    help = 'save data to file by refer argument (date of damp records)'

    def handle(self, *args, **options):
	"""
	save data to file by refer argument (date of damp records)   
	"""
	  
	try:
	    date_arg = args[1].split("-")
	    now_date = datetime(int(date_arg[2]), int(date_arg[1]), int(date_arg[0]))
	except IndexError:
	    now_date_full = datetime.now() - timedelta(days=1)
	    now_date = datetime(now_date_full.year, now_date_full.month, now_date_full.day, 0, 0, 0)
	
	try:
	    mongo_dump_file = args[0]
	except IndexError:
	    mongo_dump_file = settings.MONGO_DUMP_FILE % (now_date.day, now_date.month, now_date.year)

	try:
	    retention_interval = int(args[2])
	except IndexError:
	    retention_interval = settings.EVENTTRACKING_RETENTION_INTERVAL

	try:
	    date = now_date - timedelta( days=retention_interval )
	except TypeError:
	    raise CommandError('error - bad argument: time interval')
	
	date_format = date.strftime("%s")+'000'
	db_query = """'{"timestamp": {"$lt": {"$date": %(date)s}}}'""" % {
		  "date" : date_format
	}
	mongo_hosts = string.join(settings.MONGODB_HOSTS, ",")
	process = Popen(["mongoexport", "-q", db_query, "-h", mongo_hosts, "-d", settings.MONGODB_DB, "-c", settings.MONGODB_COLLECTION, "-o", mongo_dump_file], stdout=PIPE)
	process.communicate()

	if process.returncode != 0:
	    raise CommandError('error - bad mongo query')
	
	print'Successfull dump'


