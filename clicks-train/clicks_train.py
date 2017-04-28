import pandas as pd
import numpy as np
import time

t0 = time.clock()
filename = "clicks_train.csv"
#filename = "test.csv"
file_num = 1
#output_file = "output/clicks_train_output" + str(file_num) + ".csv"


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
	    # print "---------------------------------------------------"
	    display_ids_arr = group.sort_values(by='display_id').display_id.unique()
	    #print display_ids_arr
	    num_of_no_clicks = group['clicked'][group.clicked == 0].count()
	    num_of_yes_clicks = group['clicked'][group.clicked == 1].count()
	    # print "No clicks: %d" % num_of_no_clicks
	    # print "Yes clicks: %d" % num_of_yes_clicks
	    total = num_of_yes_clicks + num_of_no_clicks
	    ctr = num_of_yes_clicks/float(total)
	    #print "total %d yes %d ctr %d" % (total,num_of_yes_clicks, ctr)
	    
	    dictionary = {"ad_id": name, "display_ids": display_ids_arr, "yes": num_of_yes_clicks, "no": num_of_no_clicks, "total": total, "ctr": ctr}
	    list_of_dicts.append(dictionary)
	del chunk_for
	chunk1 = time.clock()
	chunk_time = chunk1 - chunk0
	print "%d seconds to process chunk %d" % (chunk_time, chunk_num)
	if((chunk_num % 500) == 0):
		output_file = "output/clicks_train_output" + str(file_num) + ".csv"
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
output_file = "output/clicks_train_output" + str(file_num) + ".csv"
output_df = pd.DataFrame(list_of_dicts)
#print output_df
output_df.to_csv(output_file, sep=',', index=False)

t1 = time.clock()
print t1 - t0, "seconds process time"

#87141732 total lines
# total_train = pd.read_csv(output_file, chunksize=10500)
# #print total_train.size()
# count =0 
# for ch in total_train:
# 	count = count +1
# 	total_check = ch['total'].sum()

# print "count %d " % (count)
# print "total %d " % (total_check)