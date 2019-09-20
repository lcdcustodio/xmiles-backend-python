import json
import urllib2
import time
import datetime as dt
import pytz
from datetime import datetime
import signal
import os
import psycopg2
import psycopg2.extras
import sys
#-----------------------------------------------
HOST = 'localhost'
DB_NAME = 'xmiles'
USER = 'power_user'
PASS = 'power_user'
#-----------------------------------------------
HISTORY_URL = "https://api.uber.com/v1.2/history"
PRODUCT_URL = "https://api.uber.com/v1.2/products/"

LIMIT = "&limit=20"
#-----------------------------------------------

SLEEP_TIME = 10 # 40 secs
#-----------------------------------------------
#DEBUG = True
DEBUG = False
#-----------------------------------------------
USER_ID = "783414521747915"
#-----------------------------------------------


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

def insert_table(query):
        conn_string = "host=%s dbname=%s user=%s password=%s" % (HOST, DB_NAME, USER, PASS)

        conn = psycopg2.connect(conn_string)

        cursor = conn.cursor()
	if DEBUG:
        	out_log.write("\n")
        	#print "Executing query: %s\n" % (query)
        	out_log.write("Executing query: %s\n" % (query))

	print "Executing query: %s\n" % (query)

        cursor.execute(query)

        conn.commit()
        print "Total number of rows updated: %s\n" % (cursor.rowcount)
	if DEBUG:
        	out_log.write("Total number of rows updated: %s\n" % (cursor.rowcount))


def mkdir(folder):
    return os.system('mkdir "%s"' % folder)



def sigint_handler(signal, frame):
        print "\n\nYou pressed Ctrl+C!, stopping script"
        print time.ctime()
        print "Exiting"
        sys.exit(0)

#Binding SIGINT to a handler, so we can gracefully stop this script
signal.signal(signal.SIGINT, sigint_handler)
#-----------------------------------------------
CollectionDate =''.join(str(datetime.now())[0:10].split("-"))
folder = "/usr/xmiles/newsfeed_update/logs/" +  CollectionDate + "_log"
out_file = folder + "/apiuber_out.log2"

if not os.path.isdir(folder):
  mkdir(folder)

out_log = open(out_file,'w')
#---------------------------------
print "Starting Uber API - Get Rides"
print time.ctime()


while True:

        CollectionDate =''.join(str(datetime.now())[0:10].split("-"))
        folder = "/usr/xmiles/newsfeed_update/logs/" +  CollectionDate + "_log"
        out_file = folder + "/apiuber_out.log2"

        if not os.path.isdir(folder):
          mkdir(folder)
          out_log = open(out_file,'w')

        out_log.close()
        out_log = open(out_file,'a')

        msg = "-------------------\nRun PostgreSQL Func at %s\n-------------------" % time.ctime()
        print msg
	if DEBUG:
        	out_log.write(msg)

 	#-------------------------
 	try:
 	
		query = "select user_id, uber_access_token, created_at from uber.access_token where uber_access_token is not null";		
		#query_result = [i for i in execute_query_db(query)]
		query_result = execute_query_db(query)
		
		for row in query_result:
			
			user_id = row['user_id']
			a_token = row['uber_access_token']
			created_at =  row['created_at']

			#print user_id
			#print a_token
 		#""" 		
			query = "select request_id from uber.rides_history where user_id = %r;" % str(user_id)		
			query_result = [i for i in execute_query_db(query)]
	  		request_id = []	
		
			for p in query_result: request_id.append(p[0])
			print request_id
 	        #"""	
			url = HISTORY_URL + "?access_token=" + a_token + LIMIT
			
			#print url

			response = urllib2.urlopen(url)
			array = json.loads(response.read())

			data = array['history']
			tz = pytz.timezone('America/Sao_Paulo')
		
			#-------------------------
			query = "insert into uber.rides_history (user_id,status,request_id,product_id,product_name,distance,flag_action,time_stamp) VALUES"
		
		
			flag_action = False	
	
			for i in range(len(data)):

				if data[i].get('request_id') not in request_id:
			
					s = data[i].get('request_time')

					stime_tz = dt.datetime.fromtimestamp(s, tz)
					stime = stime_tz.replace(tzinfo=None)
					#print stime
					if stime > created_at:			
	
						url2 = PRODUCT_URL + data[i].get('product_id') + "?access_token=" +  a_token + LIMIT

						response = urllib2.urlopen(url2)
						array = json.loads(response.read())				

						if flag_action:
                            				query = query + ","
			
						flag_action = True			
						
						query = query + " (%r,%r,%r,%r,%r,%f,%r,%r)" % (str(user_id),str(data[i].get('status')),str(data[i].get('request_id')),str(data[i].get('product_id')),str(array['short_description']), data[i].get('distance')*1.60934,"ADD",str(stime))
			
			if flag_action:
				insert_table(query)
		#"""
			#print "TEMP_LALA"
			time.sleep(SLEEP_TIME)
		
        except SystemExit:
                break
        except:
                error = "Error UBER API - Get Rides"
                print time.ctime()
                print error

 		print str(sys.exc_info())
		time.sleep(SLEEP_TIME)
                
                #print str(sys.exc_info())
		if DEBUG:
                	out_log.write(error)
                	out_log.write(str(sys.exc_info()[0]))

                continue		

