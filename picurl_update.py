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
#----------------------
import urllib2
import urllib
import json
#-----------------------------------------------
HOST = 'localhost'
DB_NAME = 'xmiles'
USER = 'power_user'
PASS = 'power_user'
#-----------------------------------------------

#SLEEP_TIME = 30 # 30 secs. 
#SLEEP_TIME = 15 # 15 secs.
SLEEP_TIME = 10 # 10 secs.
SLEEP_TIME_DAY = 86400 # 24 * 3600

#----------------------------------------------
#LOCAL_TIME = "date_trunc('sec',current_timestamp at time zone 'BRT') + interval '52 minutes'"
LOCAL_TIME = "date_trunc('sec',current_timestamp at time zone 'BRT') - interval '630 seconds'"
#----------------------------------------------

def execute_query_db(query):
	conn_string = "host=%s dbname=%s user=%s password=%s" % (HOST, DB_NAME, USER, PASS)

        #print "Connecting to database\n ->%s" % (conn_string)

        conn = psycopg2.connect(conn_string)
	#conn.commit()

        cursor = conn.cursor('cursor_find_files', cursor_factory=psycopg2.extras.DictCursor)

	print "Executing query: %s\n" % (query)

        cursor.execute(query)
	
        records = cursor.fetchall()
        #print records
	conn.commit()
        return records

def update_table(query):
	conn_string = "host=%s dbname=%s user=%s password=%s" % (HOST, DB_NAME, USER, PASS)
 
	conn = psycopg2.connect(conn_string)
 
	cursor = conn.cursor()

	out_log.write("\n") 
	print "Executing query: %s\n" % (query)
	out_log.write("Executing query: %s\n" % (query))
 
	cursor.execute(query)
 
	#records = cursor.fetchall()
	conn.commit()
	print "Total number of rows updated: %s\n" % (cursor.rowcount)
	out_log.write("Total number of rows updated: %s\n" % (cursor.rowcount))
	#return records


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
out_file = folder + "/picurl_out.log2"

if not os.path.isdir(folder):
  mkdir(folder)

out_log = open(out_file,'w')
#---------------------------------

print "Starting xmiles newsfeed_update"
print time.ctime()

while True:
#i1 = 0
#while i1 < 1:

	#i1 = i1 + 1

	CollectionDate =''.join(str(datetime.now())[0:10].split("-"))
	folder = "/usr/xmiles/newsfeed_update/logs/" +  CollectionDate + "_log"
	out_file = folder + "/picurl_out.log2" 

	if not os.path.isdir(folder):
	  mkdir(folder)
	  out_log = open(out_file,'w')

	out_log.close()
	out_log = open(out_file,'a')

	msg = "-------------------\nRun PostgreSQL Func at %s\n-------------------" % time.ctime()
	print msg
	out_log.write(msg)

        try:
                query  = "select app_dl.score_update();"
                
                result = execute_query_db(query)
                print result[0][0]
                out_log.write(result[0][0] + "\n")

                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error Score Update"
                print time.ctime()
                print error
                out_log.write(error + "\n")
                out_log.write(str(sys.exc_info()[0]) + "\n")
                continue

        try:
                query  = "select nfu.cleanup_users();"

                result = execute_query_db(query)
                print result[0][0]
                out_log.write(result[0][0] + "\n")

                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error Cleanup nfu.users"
                print time.ctime()
                print error
                out_log.write(error + "\n")
                out_log.write(str(sys.exc_info()[0]) + "\n")
                continue

        try:
                query  = "select rewards.carousel();"

                result = execute_query_db(query)
                print result[0][0]
                out_log.write(result[0][0] + "\n")

                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error update rewards.carousel"
                print time.ctime()
                print error
                out_log.write(error + "\n")
                out_log.write(str(sys.exc_info()[0]) + "\n")
                continue


        try:
                query  = "select id from newsfeed.main where destination = 'PUSH';"


                result = execute_query_db(query)

                #print result[0][0]
                #print result[0]


                row_count = 0
                for row in result:
                        row_count += 1

			print "row: %s  feed_id  %s\n" % (row_count, row[0])


			query = "update newsfeed.main set time_stamp = %s where id = %s;" % (LOCAL_TIME, row[0])

			print query
                        update_table(query)



                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error time_stamp  Update"
                print time.ctime()
                print error
                out_log.write(error + "\n")
                out_log.write(str(sys.exc_info()[0]) + "\n")
                continue


        try:
                query  = "select user_id, max(created_at), min(created_at) from users.gcm_regid group by user_id;"


                result = execute_query_db(query)

                #print result[0][0]
                #print result[0]


                row_count = 0
                for row in result:
                        row_count += 1

                        #print "row: %s  user_id:  %s max_created_at:  %s min_created_at:  %s\n" % (row_count, row[0], row[1], row[2])
                        
                        query = "delete from users.gcm_regid where user_id = '%s' and created_at < '%s';" % (row[0], row[1])

                        print query
                        update_table(query)



                time.sleep(SLEEP_TIME)


        except SystemExit:
                break
        except:
                error = "Error users.gcm_regid cleanup"
                print time.ctime()
                print error
                out_log.write(error + "\n")
                out_log.write(str(sys.exc_info()[0]) + "\n")
                continue


	
	try:
		
		query = "select * from users.profile;"
			
		result = execute_query_db(query)
		
		#print result[0][0]
		#print result[0]
		
		
		row_count = 0
		for row in result:
			row_count += 1
			#print "row: %s  user_id  %s picurl %s\n" % (row_count, row[1], row[4])
			if row[5] == 'YES':

			    try:	
			    	#url = "http://graph.facebook.com/%s/picture" % row[1]
                                url = "http://graph.facebook.com/%s/picture?type=large" % row[1]

			   	print url+"\n"
			    	response = urllib2.urlopen(url)						
			    	print response.geturl()+"\n"
	    
			    

			    	query = "update users.profile set picurl = %r, picurl_update_at = date_trunc('sec',current_timestamp at time zone 'BRT') where user_id = %r;" % (response.geturl(), row[1])
			
			    	update_table(query)
			
                            	query = "update newsfeed.main set profilepic = %r where sender = %r;" % (response.geturl(), row[1])

                            	update_table(query)
		
                            	query = "update app_dl.score set picurl = %r where user_id = %r;" % (response.geturl(), row[1])

                            	update_table(query)
		
                            except:
                                error = "Error graph.facebook.com"
                                print error

                                continue


		
		time.sleep(SLEEP_TIME_DAY)


	except SystemExit:
		break
	except:
		error = "Error PicUrl Update"
		print time.ctime()
		print error
		#print str(sys.exc_info()[0])
		print str(sys.exc_info())

		time.sleep(SLEEP_TIME)

		out_log.write(error)
		out_log.write(str(sys.exc_info()[0]))
		continue



 	msg = "-------------------\nEnd PostgreSQL Func at %s\n-------------------" % time.ctime()

	print msg
	out_log.write(msg)





