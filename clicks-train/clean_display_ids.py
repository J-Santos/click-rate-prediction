import pandas as pd
import numpy as np
import time
from ast import literal_eval
import sys
import os
from datetime import datetime

t0 = str(datetime.now())
input_file = sys.argv[1]
output_file = sys.argv[2]
list_of_files = []

def processFile(filename, output_file):
	click_train = pd.read_csv(filename, chunksize=10000)

	list_of_dicts = []
	chunk_num = 1

	for chunk in click_train:
		chunk0 = time.clock()
		#print chunk
		for (i, row) in chunk.iterrows():
			dictionary = {"ad_id": row.get('ad_id'), "yes": row.get('yes'), "no": row.get('no'), "total": row.get('total'), "ctr": row.get('ctr')}
			list_of_dicts.append(dictionary)
			del dictionary

		chunk1 = time.clock()
		chunk_time = chunk1 - chunk0
		print "%d seconds to process chunk %d." % (chunk_time, chunk_num)

		chunk_num = chunk_num + 1
		del chunk
		# if(chunk_num ==55):
		# 	break
		#break

	del click_train
	output_df = pd.DataFrame(list_of_dicts)
	output_df = output_df.sort_values(by='ctr', ascending=False)
	output_df.to_csv(output_file, sep=',', index=False)
	del output_df
	del list_of_dicts
	return;

processFile(input_file, output_file)


t1 = str(datetime.now())
print "Process completed. Start time: %s -- End time: %s" % (t0,t1)