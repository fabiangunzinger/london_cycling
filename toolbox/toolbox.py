import glob
import janitor
import pandas as pd
import numpy as np

def make_sample_data():
	"""
	Creates a 1 percent sample of the 2016 cycle hires data.
	"""
	files = glob.glob('./data/2016TripData/*.csv')

	def sec_to_min(x):
	    return x/60

	df = (
	    pd.concat([pd.read_csv(f) for f in files], 
	              ignore_index=True, sort=False)
	    .clean_names()
	    .remove_columns(['unnamed_9',
	                     'unnamed_10',
	                     'unnamed_11',
	                     'endstation_logical_terminal',
	                     'startstation_logical_terminal',
	                     'endstationpriority_id',
	                     'end_date']
	                   )
	    .rename_column('start_date', 'date')
	    .transform_column('duration', sec_to_min)
	)

	df.sample(frac = 0.1).to_csv('./data/sample2016.csv', index=False)
	'Sample saved as ./data/sample2016.csv'


