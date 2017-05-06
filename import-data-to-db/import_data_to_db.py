import pandas as pd
import numpy as np
import time
import sys
import os
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine

t0 = str(datetime.now())

#db = "cmpe239"
db = "cmpe239projectdb"


#mysql -h jesdbinstance.cimw8kngg4r8.us-west-2.rds.amazonaws.com -P 3306 -u dbuser -p
engine = create_engine("mysql+mysqldb://dbuser:239dbuser"+''+"@jesdbinstance.cimw8kngg4r8.us-west-2.rds.amazonaws.com:3306/" + db)


#engine = create_engine("mysql+mysqldb://root:"+''+"@localhost/" + db)

clicks_rate_table = 'clicksrate'
events_table = 'events'
display_id_table = 'displayid'


input_directory = sys.argv[1]
db_table = sys.argv[2]
list_of_files = []


def processFile(filename, engine, table_name):
	click_train = pd.read_csv(filename, chunksize=10000, sep=',', encoding='utf-8')

	chunk_num = 1

	for chunk in click_train:
		#print chunk
		chunk0 = time.clock()
		chunk.to_sql(table_name,engine, if_exists='append',index=False)

		chunk1 = time.clock()
		chunk_time = chunk1 - chunk0
		print "%f seconds to process chunk %d. %s" % (chunk_time, chunk_num, filename)

		chunk_num = chunk_num + 1
		del chunk

	del click_train
	return;

for i,file in enumerate(os.listdir(input_directory)):
    if file.endswith(".csv"):
        list_of_files.append(file)

for file in list_of_files:
	filename = input_directory + "/"+ file;
	processFile(filename, engine, db_table)

num_of_files = len(list_of_files)

#processFile(input_file, engine)

#conn.close()
t1 = str(datetime.now())
print "Process completed. Start time: %s -- End time: %s" % (t0,t1)