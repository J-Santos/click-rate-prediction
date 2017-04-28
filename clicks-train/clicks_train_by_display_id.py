import pandas as pd
import numpy as np
import time
from datetime import datetime

t0 = str(datetime.now())
filename = "clicks_train.csv"
#filename = "test.csv"
file_num = 1
#output_file = "output/clicks_train_output" + str(file_num) + ".csv"


click_train = pd.read_csv(filename, chunksize=10000)

list_of_dicts = []
chunk_num = 1
for chunk in click_train:
	chunk0 = time.clock()
	chunk_for = chunk.groupby('display_id')
	for name, group in chunk_for:
	    #print "Ad id: %s" % name
	    #print group
	    display_ids_group = group.sort_values(by='ad_id').ad_id.unique()
	    display_ids_arr = []

	    for val in display_ids_group:
	    	display_ids_arr.append(val)
	    dictionary = {"display_id": name, "ad_ids": np.array(display_ids_arr, dtype=int).tolist()}
	    #print dictionary
	    list_of_dicts.append(dictionary)
	del chunk_for
	chunk1 = time.clock()
	chunk_time = chunk1 - chunk0
	print "%d seconds to process chunk %d" % (chunk_time, chunk_num)
	if((chunk_num % 500) == 0):
		output_file = "clicks_train/output_display_ids/clicks_train_output" + str(file_num) + ".csv"
		output_df = pd.DataFrame(list_of_dicts)
		#print output_df
		output_df.to_csv(output_file, sep=',', index=False)
		del list_of_dicts
		list_of_dicts = []
		file_num = file_num + 1
		del output_df

	chunk_num = chunk_num + 1
	del chunk
	# if(chunk_num ==55):
	# 	break

del click_train
output_file = "clicks_train/output_display_ids/clicks_train_output" + str(file_num) + ".csv"
output_df = pd.DataFrame(list_of_dicts)
#print output_df
output_df.to_csv(output_file, sep=',', index=False)

t1 = str(datetime.now())
print "Process completed. Start time: %s -- End time: %s" % (t0,t1)
