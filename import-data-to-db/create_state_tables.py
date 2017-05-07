import time
import sys
import os
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine


if(not (len(sys.argv) > 1)):
	print "Need to specify location of db (i.e. local or cloud)"
	sys.exit()

db_location = sys.argv[1]
db = "cmpe239projectdb"

states = ["AL", "AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
			"NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","PR","RI","SC","SD","TN","TX","UT","VT","VI","VA","WA","WV","WI","WY"]

t0 = str(datetime.now())

if(db_location == 'local'):
	conn = mysql.connector.connect(user='root', database=db)
	#print "HERE"
elif(db_location == 'cloud'):
	conn = mysql.connector.connect(user='dbuser', password='239dbuser', host='jesdbinstance.cimw8kngg4r8.us-west-2.rds.amazonaws.com', port=3306, database=db)

cursor = conn.cursor()

create_table = ("CREATE Table %s as SELECT * from US_events where state = 'CA' ")

for state in states:
	t0_state = time.clock()
	table_name = state + "_events"
	create_table = ("CREATE Table "+ table_name +" as SELECT * from US_events where state = '" + state +"'")
	data = (state)
	cursor.execute(create_table)
	conn.commit()
	t1_state = time.clock()
	process_time = t1_state - t0_state
	print "%f seconds to process state %s." % (process_time, state)

##Close conections to database
cursor.close()
conn.close()
t1 = str(datetime.now())
print "Process completed. Start time: %s -- End time: %s" % (t0,t1)