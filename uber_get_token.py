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
#import requests
from requests import post
#----------------------------------------------
from uber_rides.utils.request import build_url
from uber_rides.utils import auth
#-----------------------------------------------
HOST = 'localhost'
DB_NAME = 'xmiles'
USER = 'power_user'
PASS = 'power_user'
#-----------------------------------------------
SLEEP_TIME = 3600 # 3600 secs
#-----------------------------------------------
#DEBUG = True
DEBUG = False
#-----------------------------------------------
#----------------------------------------------
#LOCAL_TIME = "date_trunc('sec',current_timestamp at time zone 'BRT') + interval '52 minutes'"
LOCAL_TIME = "date_trunc('sec',current_timestamp at time zone 'BRT') - interval '630 seconds'"
#----------------------------------------------

TOKEN_URL = 'https://login.uber.com/oauth/v2/token/'

CLIENT_SECRET = 'FaTnVYqGIUUqUl9IgLR2DrEESV_2RCvIz4PoZWmN'
CLIENT_ID = 'qimwuhpaW1P1i-XuskE7Z0z7_5iM4Eb1'
GRANT_TYPE = 'authorization_code'
REDIRECT_URI = 'https://sso.cisco.com/autho/forms/CDCANC/login.html'
#CODE = 'Udvjc00psY2jQX2dJggvrh5PxaQw55#_'
#----------------------------------------------------------

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


def update_table(query):
	conn_string = "host=%s dbname=%s user=%s password=%s" % (HOST, DB_NAME, USER, PASS)
 
	conn = psycopg2.connect(conn_string)
 
	cursor = conn.cursor()

	if DEBUG:
		out_log.write("\n") 
		print "Executing query: %s\n" % (query)
		out_log.write("Executing query: %s\n" % (query))
 
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

#
# SCRIPT FOR PARSING FILES
#
#---------------------------------
CollectionDate =''.join(str(datetime.now())[0:10].split("-"))

folder = "/usr/xmiles/newsfeed_update/logs/" +  CollectionDate + "_log"
out_file = folder + "/access_token_uber.log2"

if not os.path.isdir(folder):
  mkdir(folder)

out_log = open(out_file,'w')
#---------------------------------

print "Starting xmiles AccessToken_Uber"
print time.ctime()


while True:


        CollectionDate =''.join(str(datetime.now())[0:10].split("-"))
        folder = "/usr/xmiles/newsfeed_update/logs/" +  CollectionDate + "_log"
        out_file = folder + "/access_token_uber.log2"

        if not os.path.isdir(folder):
          mkdir(folder)
          out_log = open(out_file,'w')

        out_log.close()
        out_log = open(out_file,'a')

        msg = "-------------------\nRun PostgreSQL Func at %s\n-------------------" % time.ctime()
        print msg
	if DEBUG:
        	out_log.write(msg)

        try:

		query = "select uber_code from uber.access_token where uber_access_token is null";
		query_result = [i for i in execute_query_db(query)]
		uber_code = []
		for p in query_result: uber_code.append(p[0])
		
		for i in range(len(uber_code)):

			print uber_code[i]
			print i
		
			url = build_url(auth.AUTH_HOST, auth.ACCESS_TOKEN_PATH)


			args = {
	    		'grant_type': GRANT_TYPE,
	    		'client_id': CLIENT_ID,
	    		'client_secret': CLIENT_SECRET,    
	    		'code': uber_code[i],
	    		'redirect_uri': REDIRECT_URI,
			}

			response = post(url=url, data=args)
						
			#print response.status_code
			#print response.reason
			#print response.text
			
			json_resp = json.loads(response.text)
			#print json_resp.get('access_token')
			#print json_resp.get('refresh_token')

			if json_resp.get('access_token') is not None:
				

				query = "update uber.access_token set uber_access_token = %r, uber_refresh_token = %r, update_at = %s where uber_code = %r;" % (str(json_resp.get('access_token')), str(json_resp.get('refresh_token')), LOCAL_TIME, str(uber_code[i]))

				print query
				update_table(query)




		time.sleep(SLEEP_TIME)

        except SystemExit:
                break
        except:
                error = "Error Access Token Uber"
                print time.ctime()
                print error

		print str(sys.exc_info())

		time.sleep(SLEEP_TIME)
                
		if DEBUG:
                	out_log.write(error)
                	out_log.write(str(sys.exc_info()[0]))

                continue
