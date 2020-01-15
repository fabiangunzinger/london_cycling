import glob
import janitor
import re
import numpy as np
import pandas as pd
import geopandas as gpd
import pandas_flavor as pf

@pf.register_dataframe_method
def keep_columns(df, column_names):
    return df[column_names]


def get_hiring_data(subset=''):
	"""
	Load cycle hire data for subset of data. Cannot scrape website 
	because data is hidden, so download list of all files to csv.
	"""
	url = 'https://cycling.data.tfl.gov.uk/usage-stats/'
	csvs = open('./data/cycle_hires/hiring_data_csvs.csv', 
	            'r').read().splitlines()    
	files = [url+f for f in csvs if re.search(subset, f)]
	dfs = [pd.read_csv(f) for f in files]

	df = (
	    pd.concat(dfs, ignore_index=True, sort=False)
	    .dropna()
	    .clean_names()
	    .drop_duplicates()
	    .keep_columns(['duration', 'end_date', 'endstation_id', 'startstation_id', 'start_date'])
	    .transform_column('duration', lambda x: x/60)
	    .change_type('startstation_id', int)
	    .change_type('endstation_id', int)
	)

	df['start_date'] = pd.to_datetime(df['start_date'], 
	                                  format='%d/%m/%Y %H:%M')
	df['end_date'] = pd.to_datetime(df['end_date'],
	                                format='%d/%m/%Y %H:%M')
	return files



def make_2016sample_data():
	"""
	Creates a 1 percent sample of the 2016 cycle hires data.
	"""
	files = glob.glob('./data/cycle_hires/2016_trip_data/*.csv')

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


def get_stations_data():
	"""
	Returns dataFrame with unique entries for each station name. 
	Multiple stations per name are aggregated using Geopanda's 
	dissolve function.
	"""
	fp = "./data/geo_data/cycle_parking_maps/CycleParking 2015.TAB"
	stations = (
	    gpd.read_file(fp, driver="MapInfo File")
        .dropna()
	    .clean_names()
	    .rename_columns({'cpuniqueid':'station_id', 
	                      'number_of_parking_spaces':'parking_spaces'})
        .transform_column('station_id', lambda x: re.sub('[a-z]', '', x))
        .change_type('station_id', int)
	    .keep_columns(['station_id', 'station_name', 
	                  'parking_spaces', 'geometry'])
        .dissolve(by='station_name', aggfunc={'parking_spaces':'sum',
                                              'station_id':'first'})
        .reset_index()
	    )
	return stations


def get_borough_boundaries():
	fp = 'data/geo_data/gis_boundaries_london/ESRI/London_Borough_Excluding_MHW.shp'
	boundaries = (
	    gpd.read_file(fp)
	    .clean_names()
	    .keep_columns(['name', 'geometry'])
	)
	return boundaries


def get_borough_profiles():
	file = 'data/london_borough_profiles.csv'
	boroughs = (
	    pd.read_csv(file, header=0)
	    .rename_columns({'GLA_Population_Estimate_2017':'population', 
	                     'Average_Age,_2017':'average_age', 
	                     'Proportion_of_population_of_working-age,_2015':'prop_working_age',
	                     'Happiness_score_2011-14_(out_of_10)':'happiness',
	                     'Anxiety_score_2011-14_(out_of_10)':'anxiety',
	                     'Childhood_Obesity_Prevalance_(%)_2015/16':'childhood_obesity_preval',
	                     'Proportion_of_seats_won_by_Labour_in_2014_election':'labour_seats'})
	    .clean_names()
	    .keep_columns(['area_name','population','average_age', 
	                  'prop_working_age', 'happiness', 
	                   'anxiety', 'childhood_obesity_preval', 
	                   'labour_seats'])
	)
	return boroughs




