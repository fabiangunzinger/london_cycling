

def make_sample_data():
	"""
	Creates a 1 percent sample of the 2016 cycle hires data.
	"""
	files = glob.glob('./data/2016TripData/*.csv')
	
	# Read and clean data
	df = (
	    pd.concat([pd.read_csv(f) for f in files], ignore_index=True, sort=False)
	    .clean_names()
	)

	# Drop irrelevant columns
	df.drop(list(df.filter(regex='unnamed|logical|priority')), axis = 1, inplace = True)
	
	df.sample(frac = 0.1).to_csv('./data/sample2016.csv', index=False)

