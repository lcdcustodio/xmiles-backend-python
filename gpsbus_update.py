import os
import subprocess
import psycopg2
import psycopg2.extras
import pprint
import time
from datetime import datetime
import sys
import signal
import subprocess
#-----------------------------------------------
HOST = 'localhost'
DB_NAME = 'xmiles'
USER = 'power_user'
PASS = 'power_user'
#-----------------------------------------------
#SLEEP_TIME = 30 # 30 secs. 
SLEEP_TIME = 15 # 15 secs.
#SLEEP_TIME = 10 # 10 secs.
S_TIME_FASTER = 2 #2 secs.
#-----------------------------------------------
#DEBUG = True
DEBUG = False
#----------------------------------------------
BUSGPS_FOLDER = "/var/www/html/xmiles/busgps_files"
#----------------------------------------------

def update_table(query):
	conn_string = "host=%s dbname=%s user=%s password=%s" % (HOST, DB_NAME, USER, PASS)
 
	conn = psycopg2.connect(conn_string)
 
	cursor = conn.cursor()

	 
	print "Executing query: %s" % (query)
	if DEBUG:
		out_log.write("Executing query: %s\n" % (query))
 
	cursor.execute(query)
 
	#records = cursor.fetchall()
	conn.commit()
	print "Total number of rows updated: %s\n" % (cursor.rowcount)
	if DEBUG:
		out_log.write("Total number of rows updated: %s\n" % (cursor.rowcount))
	#return records



def execute_query_db(query):
	conn_string = "host=%s dbname=%s user=%s password=%s" % (HOST, DB_NAME, USER, PASS)

        #print "Connecting to database\n ->%s" % (conn_string)

        conn = psycopg2.connect(conn_string)
	#conn.commit()

        cursor = conn.cursor('cursor_find_files', cursor_factory=psycopg2.extras.DictCursor)

	print "Executing query: %s" % (query)

        cursor.execute(query)
        records = cursor.fetchall()
        #print records
	conn.commit()
        return records

def mkdir(folder):
    return os.system('mkdir "%s"' % folder)



def sigint_handler(signal, frame):
	print "\n\nYou pressed Ctrl+C!, stopping script"
	print time.ctime()
	print "Exiting"
   	sys.exit(0)

#Binding SIGINT to a handler, so we can gracefully stop this script
signal.signal(signal.SIGINT, sigint_handler)

#
# SCRIPT FOR PARSING FILES
#
CollectionDate =''.join(str(datetime.now())[0:10].split("-"))
folder = "/usr/xmiles/newsfeed_update/logs/" +  CollectionDate + "_log"
out_file = folder + "/busgps_out.log2"

if not os.path.isdir(folder):
  mkdir(folder)

out_log = open(out_file,'w')
#---------------------------------

print "Starting xmiles newsfeed_update"
print time.ctime()

#i1 = 0

#while i1 < 1:
while True:

	#i1 = 1
	CollectionDate =''.join(str(datetime.now())[0:10].split("-"))
	folder = "/usr/xmiles/newsfeed_update/logs/" +  CollectionDate + "_log"
	out_file = folder + "/busgps_out.log2" 

	if not os.path.isdir(folder):
	  mkdir(folder)
	  out_log = open(out_file,'w')

	msg = "-------------------\nRun PostgreSQL Func at %s\n-------------------\n" % time.ctime()
	print msg
	if DEBUG:
		out_log.write(msg)


	busgps_files = os.listdir(BUSGPS_FOLDER)
	total_files = len(busgps_files)

	for i in range(total_files):
		
		try:
			query = "COPY routes.gps_bus FROM \'" + BUSGPS_FOLDER + "/" + busgps_files[i] + "\' (DELIMITER \',\');"
			
			update_table(query)
					
			time.sleep(S_TIME_FASTER)

			os.remove(BUSGPS_FOLDER + "/" + busgps_files[i])


		except SystemExit:
			break
		except:
			error = "Error GpsBus Update"
			print time.ctime()
			print error
			if DEBUG:
				out_log.write(error + "\n")
				out_log.write(str(sys.exc_info()[0]) + "\n")

			continue

	time.sleep(SLEEP_TIME)	
