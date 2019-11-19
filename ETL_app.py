
import extract
import transform
import load
import time

total_start_time = time.time()
#EXTRACT step
print(f"BEGIN ETL JOB @ {total_start_time}")
print('-----------------------------------------------------')
datasets = {}
#extract breweries to df and store in dict
breweries_archive = 'resources/us-breweries.zip'
breweries_file = 'breweries_us.csv'

print('-----BEGIN EXTRACT STEP-----')
print(f"--EXTRACT us-breweries.zip")
breweries_df = extract.extract_zip(breweries_archive,breweries_file)

#extract reviews to df and store in dict
reviews_archive = 'resources/beerreviews.zip'
reviews_file = 'beer_reviews.csv'

print(f"--EXTRACT beerreviews.zip")
reviews_df = extract.extract_zip(reviews_archive,reviews_file)


#TRANSFORM step
print('-----BEGIN TRANSFORM STEP-----')
print('--TRANSFORM breweries_df')
breweries,brewery_locations = transform.breweries(breweries_df)

print('--TRANSFORM reviews_df')
beers,reviews = transform.reviews(reviews_df)

print('Drop reviews for unknown breweries')
reviews = reviews.merge(breweries, how='inner', left_on='brewery_id', right_on='brewery_id')


#LOAD step
print('-----BEGIN LOAD STEP-----')
print('--Build dict of datasets for export')
datasets['breweries'] = breweries
datasets['brewery_locations'] = brewery_locations
datasets['beers'] = beers
datasets['reviews'] = reviews

dbname = 'beer_data'
print(f'--LOAD to SQLite @ /resources/{dbname}.db')
load.load_db(datasets,dbname)

total_end_time = time.time()
print('-----------------------------------------------------')
print(f"END ETL JOB @ {total_start_time}")
print(f"Extract Events total: {round(total_end_time - total_start_time,4)}") 