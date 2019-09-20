
#!/usr/bin/python
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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
EMAIL_LIST = ['lcdcustodio@gmail.com','cadu@crowdmobile.com.br','rachel@crowdmobile.com.br','amanda@crowdmobile.com.br']
#EMAIL_LIST = ['amanda@crowdmobile.com.br']
#EMAIL_LIST = ['amanda@crowdmobile.com.br']
#-----------------------------------------------
SLEEP_TIME = 3590 # ~1 hour.
SLEEP_TIME_SHORT = 10 # 10 secs.

#-----------------------------------------------
#DEBUG = True
DEBUG = False
#----------------------------------------------
def prompt(prompt):
    return raw_input(prompt).strip()


#def send_email(toaddrs):
def send_email(toaddrs, time_stamp):

	fromaddr = 'lcdcustodio@gmail.com'
	print 'send_email ' + toaddrs
	msg = MIMEMultipart('alternative')
	msg['Subject'] = "Dashboard xmiles - " + time_stamp
	msg['From'] = fromaddr
	msg['To'] = toaddrs

	text = "Veja os KPIs do xmiles em:\nhttp://www.xmiles.com.br/web/dashboard/production"

	# Record the MIME types of both parts - text/plain and text/html.
	part1 = MIMEText(text, 'plain')

	# Attach parts into message container.
	# According to RFC 2046, the last part of a multipart message, in this case
	# the HTML message, is best and preferred.
	msg.attach(part1)

	#print "Message length is " + repr(len(msg))

	#Change according to your settings
	smtp_server = 'email-smtp.us-east-1.amazonaws.com'
	smtp_username = 'AKIAJA6TU5TO535WRAKQ'
	smtp_password = 'Ar7qHobe9Ry5RWGjYySw9LVX/Gq4sY+83aqh8OERiMTu'
	smtp_port = '587'
	smtp_do_tls = True

	server = smtplib.SMTP(
	    host = smtp_server,
	    port = smtp_port,
	    timeout = 10
	)
	#---------------
	#server.set_debuglevel(10)
	#---------------
	server.starttls()
	server.ehlo()
	server.login(smtp_username, smtp_password)
	server.sendmail(fromaddr, toaddrs, msg.as_string())
	server.quit()
	#print server.quit()

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
out_file = folder + "/dashboard.log2"

if not os.path.isdir(folder):
  mkdir(folder)

out_log = open(out_file,'w')
#---------------------------------

print "Starting xmiles dashboard"
print time.ctime()

while True:

	CollectionDate =''.join(str(datetime.now())[0:10].split("-"))
	folder = "/usr/xmiles/newsfeed_update/logs/" +  CollectionDate + "_log"
	out_file = folder + "/dashboard.log2" 

	if not os.path.isdir(folder):
	  mkdir(folder)
	  out_log = open(out_file,'w')

	#out_log.close()
	#out_log = open(out_file,'a')

	msg = "-------------------\nRun PostgreSQL Func at %s\n-------------------\n" % time.ctime()
	print msg
	if DEBUG:
		out_log.write(msg)
	#"""
	try:
		query  = "select dashboard.update();"
		result = execute_query_db(query)
		print result[0][0]
		if DEBUG:
			out_log.write(result[0][0] + "\n")
		
		time.sleep(SLEEP_TIME_SHORT)


	except SystemExit:
		break
	except:
		error = "Error Dashboard Update"
		print time.ctime()
		print error
		if DEBUG:
			out_log.write(error + "\n")
			out_log.write(str(sys.exc_info()[0]) + "\n")

		continue
	#"""
        try:
                #query  = "select login from dashboard.login_activity_hourly order by time_stamp desc limit 1;"
		query  = "select login, time_stamp from dashboard.login_activity_hourly order by time_stamp desc limit 1;"

                result = execute_query_db(query)
                print result[0][0]
                print ''.join(str(result[0][1]))

                if result[0][0] > 0:
                	 
                	for i in range(len(EMAIL_LIST)):
                	
                		
                		send_email(EMAIL_LIST[i], ''.join(str(result[0][1])))
				print EMAIL_LIST[i]
                
                if DEBUG:
                        out_log.write(result[0][0] + "\n")

                time.sleep(SLEEP_TIME)


        except SystemExit:
                break

        except:
                error = "Error "
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






