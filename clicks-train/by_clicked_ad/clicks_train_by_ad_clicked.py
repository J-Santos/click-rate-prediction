import pandas as pd
import numpy as np
import time
import sys
from datetime import datetime

t0 = str(datetime.now())
input_file = sys.argv[1]
output_directory = sys.argv[2]

file_num = 1

click_train = pd.read_csv(input_file, chunksize=10000)

list_of_dicts = []
chunk_num = 1
for chunk in click_train:
	chunk0 = time.clock()
	chunk_for = chunk.groupby('display_id')
	for (i, row) in chunk.iterrows():
		if(row.get('clicked') == 1):
			#print row
			dictionary = {"display_id": row.get('display_id'), "clicked_ad_id": row.get('ad_id')}
			list_of_dicts.append(dictionary)
			del dictionary

	del chunk_for
	chunk1 = time.clock()
	chunk_time = chunk1 - chunk0
	print "%f seconds to process chunk %d" % (chunk_time, chunk_num)
	if((chunk_num % 1000) == 0):
		output_file = output_directory+ "/clicks_train_by_ad_clicked_" + str(file_num) + ".csv"
		output_df = pd.DataFrame(list_of_dicts)
		output_df.to_csv(output_file, sep=',', index=False,columns=["display_id", "clicked_ad_id"])
		del list_of_dicts
		list_of_dicts = []
		file_num = file_num + 1
		del output_df

	chunk_num = chunk_num + 1
	del chunk

del click_train
output_file = output_directory+ "/clicks_train_by_ad_clicked_" + str(file_num) + ".csv"
output_df = pd.DataFrame(list_of_dicts)
#print output_df
output_df.to_csv(output_file, sep=',', index=False,columns=["display_id", "clicked_ad_id"])

t1 = str(datetime.now())
print "Process completed. Start time: %s -- End time: %s" % (t0,t1)