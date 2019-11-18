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
us_breweries_src = 'resources/us-breweries.zip'

#unzip and import contents into dataframe
with zipfile.ZipFile(us_breweries_src) as z:
   with z.open("breweries_us.csv") as f:
      us_breweries_src_df = pd.read_csv(f)

us_breweries_src_df = etl.clean_co_names(us_breweries_src_df,'brewery_name').set_index('clean_co',drop=True)

#split out address into parts
us_breweries_src_df.rename(columns={"address": "address_src"},inplace=True)
us_breweries_src_df[['address','city','state','zip']] = us_breweries_src_df['address_src'].str.rsplit(',',n=3,expand=True)

#strip whitespace from ALL cols
col_list = [col for col in us_breweries_src_df.select_dtypes(include='object')]
us_breweries_src_df = etl.strip_whitespace(us_breweries_src_df,col_list)

#reorder columns
us_breweries_src_df = us_breweries_src_df[['brewery_name', 'type', 'address', 'city', 'state', 'zip', 'website', 'state_breweries']]

#determine closed locations
us_breweries_src_df['closed'] = us_breweries_src_df['type'].str.contains('Closed')
#strip -Closed from type
us_breweries_src_df['type'] = us_breweries_src_df['type'].str.replace('-Closed','')

#clean bad websites
us_breweries_src_df.loc[us_breweries_src_df['website'].str.contains(' ',na=False),'website'] = np.nan
us_breweries_src_df['website'] = us_breweries_src_df['website'].replace('-',np.nan)
us_breweries_src_df['website'] = us_breweries_src_df['website'].str.lower()
us_breweries_src_df['website'] = us_breweries_src_df['website'].str.replace('http://','')
us_breweries_src_df['website'] = us_breweries_src_df['website'].str.replace('https://','')
us_breweries_src_df['website'] = us_breweries_src_df['website'].str.rstrip('/')
us_breweries_src_df['bad_website'] = us_breweries_src_df['website'].isnull()


#handle invalid addresses
us_breweries_src_df_bad_address = us_breweries_src_df[us_breweries_src_df[['city','state','zip']].isnull().any(axis=1)]
us_breweries_src_df['bad_address'] = us_breweries_src_df[['city','state','zip']].isnull().any(axis=1)
us_breweries_src_df.loc[us_breweries_src_df['bad_address'] == True, ['address','city','state','zip']] = np.nan


us_breweries_src_df.info()
#reset index to build our datasets
us_breweries_src_df.reset_index(drop=False,inplace=True) #need to pull our clean_co
us_breweries_src_df.reset_index(drop=False,inplace=True) #need to pull out new ordinal index to rename location_id
us_breweries_src_df.rename(columns={"clean_co": "brewery_id","index": "location_id"},inplace=True)


#build our seperate datasets
datasets = {}
datasets['breweries'] = us_breweries_src_df[['brewery_id']].drop_duplicates().set_index('brewery_id')
datasets['brewery_locations'] = us_breweries_src_df[['location_id','type','address','city','state','zip','website','closed','bad_website','bad_address','brewery_id']].set_index('location_id')

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

##build dict of addresses
##drop index to include in dict
##us_breweries_src_df.reset_index(drop=False,inplace=True)
#
##create dict from cols
#us_breweries_src_df['addresses'] = us_breweries_src_df[['type', 'address', 'website', 'state', 'city', 'zip', 'state_breweries','closed','bad_website','bad_address']].to_dict(orient='records')
#
#drop_cols = ['type', 'address', 'website', 'state', 'city', 'zip', 'state_breweries','closed','bad_website','bad_address']
#us_breweries_src_df.drop(columns=drop_cols,inplace=True)
#
#us_breweries_src_df['addresses']  = us_breweries_src_df.to_dict(orient='records')
##drop duplicates
##us_breweries_src_df.drop_duplicates('clean_co',inplace=True)
##us_breweries_src_df.set_index('clean_co',inplace=True)
#us_breweries_src_df['addresses']  = us_breweries_src_df.groupby('clean_co')['addresses'].apply(list)
#
#us_breweries_src_df.reset_index(drop=False,inplace=True)
#
#us_breweries_src_df.drop_duplicates('clean_co',inplace=True)
#us_breweries_src_df.set_index('clean_co',inplace=True, drop=False)
#text = us_breweries_src_df.to_dict(orient='index')
##us_breweries_src_df.reset_index(drop=False,inplace=True)
##us_breweries_src_df.set_index('clean_co',inplace=True)
#
#us_breweries_src_df['location_count'] = us_breweries_src_df.groupby('bad_address').count()
#us_breweries_src_df['url_count']=us_breweries_src_df.groupby('clean_co').count()
#
#
#us_breweries_src_df['locations'] = us_breweries_src_df[['brewery_name','type', 'address', 'website', 'state', 'city', 'zip', 'state_breweries','closed','bad_website','bad_address']].to_dict(orient='records')
#us_breweries_src_df['loc_id'] = us_breweries_src_df.groupby('clean_co')['brewery_name'].cumcount()
#
#text = us_breweries_src_df['locations'].unstack()
#test = text.to_dict(orient='index')
#
#us_breweries_src_df.set_index('loc_id',append=True,inplace=True)
#
#
#us_breweries_src_df['locations']  = us_breweries_src_df.groupby('clean_co')['locations'].apply(list)
#us_breweries_src_df.reset_index(drop=False,inplace=True)
#us_breweries_src_df.drop_duplicates('clean_co',inplace=True)
#us_breweries_src_df.set_index('clean_co',inplace=True)
#text = us_breweries_src_df[['locations']].to_dict(orient='index')
#
#
#text = us_breweries_src_df[['brewery_name','addresses']].to_dict(orient='records')
#
#us_breweries_src_df['loc_id'] = us_breweries_src_df.groupby('clean_co')['brewery_name'].cumcount()