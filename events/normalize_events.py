import pandas as pd
import numpy as np
import time
from ast import literal_eval
import sys
import os
#from datetime import datetime
import datetime

t0 = str(datetime.datetime.now())
input_file = sys.argv[1]
output_dir = sys.argv[2]
list_of_files = []

def processFile(filename, output_dir):
	events = pd.read_csv(filename, chunksize=10000)

	list_of_dicts = []
	chunk_num = 1
	time_offset = 1465876799998/1000
	file_num = 1
	for chunk in events:
		chunk0 = time.clock()
		#print chunk
		for (i, row) in chunk.iterrows():
			try:
				date = datetime.datetime.fromtimestamp(time_offset+row.get('timestamp'))
				valid = True
			except ValueError, e:
				valid = False
				pass
			if(valid):
				day = datetime.datetime.fromtimestamp(time_offset+row.get('timestamp')).day
				hour = datetime.datetime.fromtimestamp(time_offset+row.get('timestamp')).hour
				minute = datetime.datetime.fromtimestamp(time_offset+row.get('timestamp')).minute
				# 0=monday, 1=tuesday ...
				weekday = datetime.datetime.fromtimestamp(time_offset+row.get('timestamp')).weekday()
			else:
				day = 'none'
				hour = 'none'
				minute = 'none'
				weekday = 'none'
			geo_location = row.get('geo_location')
			country = 'none'
			state = 'none'
			dma = 'none'

			arr = []
			if(type(geo_location) ==  str):
				arr = geo_location.split('>')
				if(len(arr) == 1):
					country = arr[0]
				elif(len(arr) == 2):
					country = arr[0]
					state = arr[1]
				elif(len(arr) == 3):
					country = arr[0]
					state = arr[1]
					dma = arr[2]
			#print geo_location
			#print "Country: %s -- State: %s -- DMA: %s" % (country, state, dma)
			#display_id,uuid,document_id,timestamp,platform,geo_location
			dictionary = {"display_id": row.get('display_id'), "uuid": row.get('uuid'), "document_id": row.get('document_id'), "platform": row.get('platform'), "country": country, "state": state, "dma": dma, "day": day, "hour": hour, "minute":minute, "weekday": weekday}
			list_of_dicts.append(dictionary)
			del dictionary, row, day, hour, minute, weekday, geo_location, country, state, dma, arr

		chunk1 = time.clock()
		chunk_time = chunk1 - chunk0
		print "%f seconds to process chunk %d." % (chunk_time, chunk_num)

		if((chunk_num % 500) == 0):
			output_file = output_dir + "/events_normalized" + str(file_num) + ".csv"
			output_df = pd.DataFrame(list_of_dicts)
			#print output_df
			output_df = output_df.sort_values(by='display_id', ascending=True)
			output_df.to_csv(output_file, sep=',', index=False, columns=["display_id", "uuid", "document_id", "platform", "country", "state", "dma", "day", "hour", "minute", "weekday"])	
			del list_of_dicts
			list_of_dicts = []
			file_num = file_num + 1
			del output_df

		chunk_num = chunk_num + 1
		del chunk

	del events
	output_df = pd.DataFrame(list_of_dicts)
	output_file = output_dir + "/events_normalized_last.csv"
	#output_df = output_df.sort_values(by='ctr', ascending=False)
	output_df = output_df.sort_values(by='display_id', ascending=True)
	output_df.to_csv(output_file, sep=',', index=False, columns=["display_id", "uuid", "document_id", "platform", "country", "state", "dma", "day", "hour", "minute", "weekday"])
	del output_df
	del list_of_dicts
	return;

processFile(input_file, output_dir)


t1 = str(datetime.datetime.now())
print "Process completed. Start time: %s -- End time: %s" % (t0,t1)