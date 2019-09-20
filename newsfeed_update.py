# -*- coding: utf-8 -*-

from gcm import GCM
#----------------------

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
API_KEY = "AIzaSyB6842VZFONtuKZOokkehAX0M0al_QdubU"
#-----------------------------------------------
#SLEEP_TIME = 30 # 30 secs. 
#SLEEP_TIME = 15 # 15 secs.
SLEEP_TIME = 10 # 10 secs.
#SLEEP_TIME = 5 # 5 secs.
#-----------------------------------------------
#DEBUG = True
DEBUG = False
#----------------------------------------------

def execute_push_msg(api_key, feed_id, push_msg, reg_id):

	
        registration_ids = []
	registration_ids.append(reg_id)
	
	gcm = GCM(api_key)

	msg = {
	    "price": str(feed_id) + ";" + push_msg   
	}

	
	response = gcm.json_request(registration_ids=registration_ids,
        	                    data=msg,
                	            collapse_key='awesomeapp_update',
                 		    priority='high',
	                            delay_while_idle=False)

	
	if DEBUG:
        	out_log.write("msg details: %s,%s\n" % (feed_id, push_msg))
	

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
#---------------------------------
CollectionDate =''.join(str(datetime.now())[0:10].split("-"))
folder = "/usr/xmiles/newsfeed_update/logs/" +  CollectionDate + "_log"
out_file = folder + "/out.log2"

if not os.path.isdir(folder):
  mkdir(folder)

out_log = open(out_file,'w')
#---------------------------------

print "Starting xmiles newsfeed_update"
print time.ctime()

while True:

	CollectionDate =''.join(str(datetime.now())[0:10].split("-"))
	folder = "/usr/xmiles/newsfeed_update/logs/" +  CollectionDate + "_log"
	out_file = folder + "/out.log2" 

	if not os.path.isdir(folder):
	  mkdir(folder)
	  out_log = open(out_file,'w')

	#out_log.close()
	#out_log = open(out_file,'a')

	msg = "-------------------\nRun PostgreSQL Func at %s\n-------------------\n" % time.ctime()
	print msg
	if DEBUG:
		out_log.write(msg)
	
	try:
		query  = "select ctrl.buscode_not_found_update();"
		#query  = "select now();"
		result = execute_query_db(query)
		#execute_query_db(query)
		print result[0][0]
		if DEBUG:
			out_log.write(result[0][0] + "\n")
		
		time.sleep(SLEEP_TIME)


	except SystemExit:
		break
	except:
		error = "Error Buscode not found Update"
		print time.ctime()
		print error
		if DEBUG:
			out_log.write(error + "\n")
			out_log.write(str(sys.exc_info()[0]) + "\n")

		continue

 	try:
                query  = "select newsfeed.feed_update();"
                #query  = "select now();"
                result = execute_query_db(query)
                #execute_query_db(query)
                print result[0][0]
                if DEBUG:
                        out_log.write(result[0][0] + "\n")

                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error NewsFeed Update"
                print time.ctime()
                print error
                if DEBUG:
                        out_log.write(error + "\n")
                        out_log.write(str(sys.exc_info()[0]) + "\n")

                continue



        try:
                query  = "select app_dl.history_update();"
                #query  = "select now();"
                result = execute_query_db(query)
                #execute_query_db(query)
                print result[0][0]
		if DEBUG:
                	out_log.write(result[0][0] + "\n")

                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error history Update"
                print time.ctime()
                print error
		if DEBUG:
                	out_log.write(error + "\n")
                	out_log.write(str(sys.exc_info()[0]) + "\n")
                continue

	#"""
        try:
                query  = "select newsfeed.likes_update();"
                
                result = execute_query_db(query)
                
                print result[0][0]
		if DEBUG:
                	out_log.write(result[0][0] + "\n")

                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error Likes Update"
                print time.ctime()
                print error
		if DEBUG:
                	out_log.write(error + "\n")
                	out_log.write(str(sys.exc_info()[0]) + "\n")
                continue
	
        try:
                query  = "select newsfeed.comments_update();"
                
                result = execute_query_db(query)
                
                print result[0][0]
		if DEBUG:
                	out_log.write(result[0][0] + "\n")

                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error Comments Update"
                print time.ctime()
                print error
		if DEBUG:
                	out_log.write(error + "\n")
                	out_log.write(str(sys.exc_info()[0]) + "\n")

                continue
	#"""
        try:
                query  = "select nfu.cleanup_rank_factor();"

                result = execute_query_db(query)

                print result[0][0]
                if DEBUG:
                        out_log.write(result[0][0] + "\n")

                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error Rank Factor Update"
                print time.ctime()
                print error
                if DEBUG:
                        out_log.write(error + "\n")
                        out_log.write(str(sys.exc_info()[0]) + "\n")

                continue

        try:
                query  = "select nfu.cleanup_friends_list();"

                result = execute_query_db(query)

                print result[0][0]
                if DEBUG:
                        out_log.write(result[0][0] + "\n")

                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error Cleanup Friends list"
                print time.ctime()
                print error
                if DEBUG:
                        out_log.write(error + "\n")
                        out_log.write(str(sys.exc_info()[0]) + "\n")

                continue
	"""
        try:
                query  = "select newsfeed.comments_2nd_order_update();"

                result = execute_query_db(query)

                print result[0][0]
		if DEBUG:
                	out_log.write(result[0][0] + "\n")

                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error Comments 2nd Order update"
                print time.ctime()
                print error
		if DEBUG:
                	out_log.write(error + "\n")
                	out_log.write(str(sys.exc_info()[0]) + "\n")

                continue
	"""



	try:
		 	
		query = "select a.id, a.feed_id, a.status, b.user_id, b.name, b.gcm_regid from newsfeed.push_notification a join users.gcm_regid b on (a.destination = b.user_id) where a.flag_action = 'WAIT'limit 1;"	
	
		result = execute_query_db(query)
		
		#";".join(a)
		id	 = int(result[0][0])
		feed_id  = result[0][1]
		push_msg = result[0][2]
		reg_id   = result[0][5]

		execute_push_msg(API_KEY, feed_id, push_msg, reg_id)
		
		query = "update newsfeed.push_notification  set flag_action = 'DONE' where id = %r;" % id
			
		update_table(query)
		             		
                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error Push Notification"
                print time.ctime()
                print error
		if DEBUG:
                	out_log.write(error + "\n")
                	out_log.write(str(sys.exc_info()[0]) + "\n")

                continue



 	msg = "-------------------\nEnd PostgreSQL Func at %s\n-------------------\n" % time.ctime()

	print msg
	if DEBUG:
		out_log.write(msg)





