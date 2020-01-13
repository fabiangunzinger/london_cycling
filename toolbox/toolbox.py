import glob
import janitor
import pandas as pd
import numpy as np

def make_sample_data():
	"""
	Creates a 1 percent sample of the 2016 cycle hires data.
	"""

	files = glob.glob('./data/2016TripData/*.csv')
	frames= [pd.read_csv(f) for f in files]
	drop_vars = 'unnamed_ | _logical_terminal | endstationpriority_'

	df = (
	    pd.concat(frames, ignore_index=True, sort=False)
	    .clean_names()
	    .remove_columns(list(df.filter(regex=drop_vars)))
	    .transform_column('duration', lambda x: x/60)
	)

	df.sample(frac = 0.1).to_csv('./data/sample2016.csv', index=False)
	return 'Sample saved as ./data/sample2016.csv'

