import pandas as pd
import numpy as np
import time
from ast import literal_eval
import sys
import os
from datetime import datetime

t0 = str(datetime.now())
input_directory = sys.argv[1]
output_directory = sys.argv[2]
list_of_files = []

def processFile(filename, output_file, filenum, filetotal):
	click_train = pd.read_csv(filename, chunksize=10000)

	list_of_dicts = []
	chunk_num = 1
	for chunk in click_train:
		chunk0 = time.clock()
		#chunk_cur = (chunk[chunk.clicked == 1]).groupby('ad_id')['clicked'].value_counts()
		chunk_for = chunk.groupby('ad_id')
		#print chunk_for.size()

		for name, group in chunk_for:
		    # print "Ad id: %s" % name
		    # print group

		    display_ids_arr = []

		    num_of_no_clicks = group['no'].sum()
		    num_of_yes_clicks = group['yes'].sum()
		    total = group['total'].sum()
		    #print float(group['ctr'].sum()/group['ctr'].count())
		    average_ctr = group['ctr'].mean()#float(group['ctr'].sum()/group['ctr'].count())
		    # print "No clicks: %d" % num_of_no_clicks
		    # print "Yes clicks: %d" % num_of_yes_clicks
		    # print "Total clicks: %d" % total
		    # print "Average CTR: %f" % average_ctr
		    # print "Display Ids: %d" % display_ids_arr

		    for (i, row) in group['display_ids'].iteritems():
		    	arr_str = ",".join(row.split())
		    	arr_str = arr_str.replace("[,", "[")
		    	arr_str = arr_str.replace(",]", "]")
		    	display_ids_arr = display_ids_arr + np.array(literal_eval(arr_str)).tolist()
		    dictionary = {"ad_id": name, "display_ids": display_ids_arr, "yes": num_of_yes_clicks, "no": num_of_no_clicks, "total": total, "ctr": average_ctr}
		    list_of_dicts.append(dictionary)
		    del dictionary

		del chunk_for
		chunk1 = time.clock()
		chunk_time = chunk1 - chunk0
		print "%d seconds to process chunk %d in file %d ouf %d: %s" % (chunk_time, chunk_num, filenum, filetotal, filename)

		chunk_num = chunk_num + 1
		del chunk
		# if(chunk_num ==55):
		# 	break
		#break

	del click_train
	#output_file = "output/reduce/clicks_train_output133.csv"
	output_df = pd.DataFrame(list_of_dicts)
	#print output_df
	output_df.to_csv(output_file, sep=',', index=False)
	del output_df
	del list_of_dicts
	return;

for i,file in enumerate(os.listdir(input_directory)):
    if file.endswith(".csv"):
        #print os.path.basename(file)
        list_of_files.append(file)
        #print file

num_of_files = len(list_of_files)
for x in range(num_of_files):
	fileStartTime = time.clock()
	inputFile = input_directory + "/" + list_of_files[x]
	outputFile = output_directory + "/" + list_of_files[x]

	processFile(inputFile, outputFile, x+1, num_of_files)
	fileEndTime = time.clock()
	print fileEndTime - fileStartTime, "seconds process time for file %s. %d out of %d files." % (list_of_files[x], x+1, num_of_files )

t1 = str(datetime.now())
print "Process completed. Start time: %s -- End time: %s" % (t0,t1)