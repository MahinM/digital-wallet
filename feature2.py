import pandas as pd
import numpy as np
from datetime import datetime
import os
import networkx as nx
#import collections 
import cProfile

"""Read file. Return dataframe.  Last field can have
embedded commas"""
def read_file(path):
    with open(path,'r') as f:
        header = [item.strip() for item in next(f).split(',')]
        lines = (line.split(',',len(header)-1) for line in f)
        df = pd.DataFrame(lines,columns=header,)
    return df

'''Parse transaction.  Reject if timestamp is not valid or
if sender or recipient ids are not numeric.  Otherwise,
transaction is valid if it's between friends of friends'''
def feature2_status(trans):
    try:
        datetime.strptime(trans.time, '%Y-%m-%d %H:%M:%S')
        sender_id = int(trans.id1)
        recipient_id = int(trans.id2)
    except (ValueError, TypeError) as err:
        return 'invalid'
    try:
        if nx.astar_path_length(g,sender_id,recipient_id) <= 2:
            return 'verified'
        else:
            return 'unverified'
    except nx.NetworkXNoPath:
        return 'unverified'
    return 'Error'

if __name__ == '__main__':
	
	###Read batch data.  Create payment history network between senders and receivers
	batch_data = read_file(os.path.join("paymo_input","batch_payment.csv"))

	payment_history = batch_data[['id1','id2']][batch_data.id1.str.strip().str.isnumeric() & batch_data.id2.str.strip().str.isnumeric()]
	payment_history = payment_history.apply(pd.to_numeric)
	connected = payment_history.drop_duplicates()
	#Note: Need to look for mirrored transactions??
	g = nx.from_pandas_dataframe(connected,'id2','id1')

	stream_data = read_file(os.path.join("paymo_input","stream_payment.csv"))
	result = stream_data.apply(lambda x: feature2_status(x),axis=1)

	#write result to file