# -*- coding: utf-8 -*-
"""
Created on Sun Nov 17 11:46:07 2019

@author: rmr
"""

#import our required modules
import pandas as pd
import zipfile
#import our custom module for etl functions
import etl_tools as etl
import numpy as np
import sqlite3 as sq
#import validators


# set our variables for our source .zip files
beer_reviews_src = 'resources/beerreviews.zip'


#unzip and import contents into dataframe
with zipfile.ZipFile(beer_reviews_src) as z:
   with z.open("beer_reviews.csv") as f:
      beer_reviews_src_df = pd.read_csv(f)
      

#Build df of names and id's to normalize (it's )
brewery_names_df = beer_reviews_src_df[['brewery_id','brewery_name']].copy()

#drop duplicates
brewery_names_df.drop_duplicates('brewery_name',inplace=True)
brewery_names_df.set_index('brewery_id',inplace=True)

#clean names
brewery_names_df = etl.clean_co_names(brewery_names_df,'brewery_name')
#drop duplicates after cleanup
brewery_names_df.drop_duplicates('clean_co',inplace=True)

#join our names back in to beer_revieww_src_df
beer_reviews_src_df = beer_reviews_src_df.merge(brewery_names_df['clean_co'],left_on='brewery_id',right_index=True)

#transform epoch to datetime
beer_reviews_src_df['review_time'] = pd.to_datetime(beer_reviews_src_df['review_time'],unit='s')

#strip whitespace from ALL cols
beer_reviews_src_df.set_index('clean_co',append=True,inplace=True)
col_list = [col for col in beer_reviews_src_df.select_dtypes(include='object')]
beer_reviews_src_df = etl.strip_whitespace(beer_reviews_src_df,col_list)

beer_reviews_src_df.reset_index(drop=False,inplace=True)
beer_reviews_src_df.rename(columns={"level_0": "review_id","brewery_id": "brewery_id_src","clean_co": "brewery_id","beer_beerid": "beer_id"},inplace=True)

#beer_reviews_src_df.info()

#build our seperate datasets
datasets = {}
datasets['beers'] = beer_reviews_src_df[['beer_id','beer_name','beer_style','beer_abv']].set_index('beer_id')
datasets['reviews'] = beer_reviews_src_df[['review_id','review_profilename','review_time','review_overall','review_aroma','review_appearance','review_palate','review_taste','beer_id','brewery_id']].set_index('review_id')

#export to sqlite
table_list = [table for table in datasets.keys()]
dbname = 'beer_data'
sql_data = f'data/{dbname}.db'

conn = sq.connect(sql_data)
cur = conn.cursor()
for table in table_list:
    dropstring = '''drop table if exists "{0}"'''.format(table)
    print(dropstring)
    cur.execute(dropstring)
    datasets[table].to_sql(table,conn, if_exists='replace', index=True)
conn.commit()
conn.close() 
