"""
This file takes records from mongo, that are elderly then date argument (first parametr) and save it in file, second parametr is date now
"""
import sys
import string
from datetime import datetime, timedelta
from subprocess import Popen, PIPE

from django.core.management.base import BaseCommand, CommandError


MONGODB_DB = 'events'
MONGODB_COLLECTION = 'events'
DUMP_FILE = 'dump.dmp'


class Command(BaseCommand):
    args = '<timedelta(days) date_now(dd-MM-YYYY) ...>'
    help = 'save data to file by refer argument (date of damp records)'

    def handle(self, *args, **options):
    """
    save data to file by refer argument (date of damp records)   
    """
	date_arg = args[2].split("-")
	now_date = datetime(int(date_arg[2]), int(date_arg[1]), int(date_arg[0]))
	try:
	    date = now_date - timedelta( days=int(args[1]) )
	except TypeError:
	    raise CommandError('error - bad time argument')
	
	date_format = date.strftime("%s")+'000'
	db_query = """'{"timestamp": {"$lt": {"$date": %(date)s}}}'""" % {
		  "date" : date_format
	}

	output_shell = Popen(["mongoexport", "-q", db_query, "-d", MONGODB_DB, "-c", MONGODB_COLLECTION, "-o", DUMP_FILE], stdout=PIPE).returncode
	if output_shell != 0:
	    raise CommandError('error - bad mongo query')
	
	self.stdout.write('Successfull dump')


