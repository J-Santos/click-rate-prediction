import pandas as pd
import numpy as np
import time
from ast import literal_eval
import sys
import os
from datetime import datetime

t0 = str(datetime.now())
input_directory = sys.argv[1]
output_file = sys.argv[2]
list_of_files = []

def processDataframe(dataframe):
	#click_train = pd.read_csv(filename, chunksize=100)

	list_of_dicts = []
	chunk_num = 1
	df0 = time.clock()
	chunk_for = dataframe.groupby('display_id')

	for name, group in chunk_for:
		#print "Ad id: %s" % name
		#print group
		chunk0 = time.clock()
		ad_ids_arr = []

		for (i, row) in group['ad_ids'].iteritems():
			ad_ids_arr = ad_ids_arr + np.array(literal_eval(row)).tolist()

		
		dictionary = {"display_id": name, "ad_ids": np.array(ad_ids_arr, dtype=int).tolist()}

		list_of_dicts.append(dictionary)
		chunk1 = time.clock()
		chunk_time = chunk1 - chunk0;
		del dictionary
		print "%f seconds to process chunk %d" % (chunk_time, chunk_num)
		chunk_num = chunk_num + 1

	del chunk_for
	df1 = time.clock()
	df_time = df1 - df0


	#output_file = output_directory + "/clicks_train_combined4.csv"
	output_df = pd.DataFrame(list_of_dicts)
	#print output_df
	output_df.to_csv(output_file, sep=',', index=False)
	del output_df
	del list_of_dicts
	return;

def combineDataFrames(list_of_frames):
	frames = []
	for frame in list_of_frames:
		frames.append(frame)
	combined = pd.concat(frames)
	return combined

for i,file in enumerate(os.listdir(input_directory)):
    if file.endswith(".csv"):
        #print os.path.basename(file)
        list_of_files.append(file)
        #print file

num_of_files = len(list_of_files)
dfs = []
for x in range(num_of_files):
	fileStartTime = time.clock()
	inputFile = input_directory + "/" + list_of_files[x]
	#outputFile = output_directory# + "/" + list_of_files[x]

	dataChunks = pd.read_csv(inputFile, chunksize=10000)
	#processFile(inputFile, outputFile, x+1, num_of_files)

	dfs.append(pd.concat(dataChunks))
	del dataChunks
	fileEndTime = time.clock()
	print fileEndTime - fileStartTime, "seconds process time for file %s. %d out of %d files." % (list_of_files[x], x+1, num_of_files )

dfCombined = combineDataFrames(dfs)
del dfs
processDataframe(dfCombined)
#dfCombined.to_csv("test/combine/output/combinedfs.csv", sep=',', index=False)
del dfCombined


t1 = str(datetime.now())
print "Process completed. Start time: %s -- End time: %s" % (t0,t1)