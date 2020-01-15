import glob
import janitor
import numpy as np
import pandas as pd
import geopandas as gpd

def make_sample_data():
	"""
	Creates a 1 percent sample of the 2016 cycle hires data.
	"""
	files = glob.glob('./data/2016_trip_data/*.csv')

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
	fp = "./data/geo_data/cycle_parking_maps/CycleParking 2015.TAB"
	stations = (
	    gpd.read_file(fp, driver="MapInfo File")
	    .clean_names()
	    .rename_columns({'cpuniqueid':'station_id', 
	                      'number_of_parking_spaces':'parking_spaces'})
	    .keep_columns(['station_id', 'station_name', 
	                  'parking_spaces', 'geometry'])
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




