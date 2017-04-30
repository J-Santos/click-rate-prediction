import pandas as pd
import numpy as np
import time
#from ast import literal_eval
import sys
#import os
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine

t0 = str(datetime.now())

engine = create_engine("mysql+mysqldb://root:"+''+"@localhost/cmpe239")


input_file = sys.argv[1]
#output_file = sys.argv[2]
list_of_files = []


def processFile(filename, engine):
	click_train = pd.read_csv(filename, chunksize=10000)

	chunk_num = 1

	for chunk in click_train:
		chunk0 = time.clock()
		chunk.to_sql('clicks',engine, if_exists='append',index=False)

		chunk1 = time.clock()
		chunk_time = chunk1 - chunk0
		print "%f seconds to process chunk %d." % (chunk_time, chunk_num)

		chunk_num = chunk_num + 1
		del chunk

	del click_train
	return;

processFile(input_file, engine)

#conn.close()
t1 = str(datetime.now())
print "Process completed. Start time: %s -- End time: %s" % (t0,t1)