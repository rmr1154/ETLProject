import etl_tools as etl
import pandas as pd
import numpy as np

def breweries(df):
    print('Clean Company Names')
    df = etl.clean_co_names(df,'brewery_name').set_index('clean_co',drop=True)
    
#split out address into parts
    print('Split Location Addresses') 
    df.rename(columns={"address": "address_src"},inplace=True)
    df[['address','city','state','zip']] = df['address_src'].str.rsplit(',',n=3,expand=True)

#strip whitespace from ALL cols
    print('Strip Whitespace (all columns)')
    col_list = [col for col in df.select_dtypes(include='object')]
    df = etl.strip_whitespace(df,col_list)
    
#reorder columns
    print('Reorder Columns')
    df = df[['brewery_name', 'type', 'address', 'city', 'state', 'zip', 'website', 'state_breweries']]

#determine closed locations
    print('Determine Closed Locations')
    df['closed'] = df['type'].str.contains('Closed')
    #strip -Closed from type
    df['type'] = df['type'].str.replace('-Closed','')
    
#clean bad websites
    print('Clean Website Addresses')
    df.loc[df['website'].str.contains(' ',na=False),'website'] = np.nan
    df['website'] = df['website'].replace('-',np.nan)
    df['website'] = df['website'].str.lower()
    df['website'] = df['website'].str.replace('http://','')
    df['website'] = df['website'].str.replace('https://','')
    df['website'] = df['website'].str.rstrip('/')
    print('Flag Bad Website Addresses')
    df['bad_website'] = df['website'].isnull()
    
    
#handle invalid addresses
    print('Flag Bad Location Addresses')
    #us_breweries_src_df_bad_address = us_breweries_src_df[us_breweries_src_df[['city','state','zip']].isnull().any(axis=1)]
    df['bad_address'] = df[['city','state','zip']].isnull().any(axis=1)
    df.loc[df['bad_address'] == True, ['address','city','state','zip']] = np.nan
    
    
#df.info()
#reset index to build our datasets
    print('Set Final Indexes and Column Names')
    df.reset_index(drop=False,inplace=True) #need to pull our clean_co
    df.reset_index(drop=False,inplace=True) #need to pull out new ordinal index to rename location_id
    df.rename(columns={"clean_co": "brewery_id","index": "location_id"},inplace=True)

    breweries = df[['brewery_id']].drop_duplicates().set_index('brewery_id')
    brewery_locations = df[['location_id','type','address','city','state','zip','website','closed','bad_website','bad_address','brewery_id']].set_index('location_id')


    return(breweries,brewery_locations)

def reviews(df):
#Build df of names and id's to normalize (it's )
    print('Create brewery_names_df to speed up etl.clean_co_names() processing')
    brewery_names_df = df[['brewery_id','brewery_name']].copy()
    
#drop duplicates
    print('Drop Duplicate brewery_name')
    brewery_names_df.drop_duplicates('brewery_name',inplace=True)
    brewery_names_df.set_index('brewery_id',inplace=True)
    
#clean names
    print('Clean Company Names')
    brewery_names_df = etl.clean_co_names(brewery_names_df,'brewery_name')
#drop duplicates after cleanup
    print('Drop Duplicate Names after etl.clean_co_names()')
    brewery_names_df.drop_duplicates('clean_co',inplace=True)
    
#join our names back in to beer_revieww_src_df
    print('Join brewery_names_df back in')
    df = df.merge(brewery_names_df['clean_co'],left_on='brewery_id',right_index=True)
    
#transform epoch to datetime
    print('Transform review_time from epoch to datetime')
    df['review_time'] = pd.to_datetime(df['review_time'],unit='s')
    
#strip whitespace from ALL cols
    print('Strip Whitespace (all columns)')
    df.set_index('clean_co',append=True,inplace=True)
    col_list = [col for col in df.select_dtypes(include='object')]
    df = etl.strip_whitespace(df,col_list)
    
    print('Set Final Indexes and Column Names')
    df.reset_index(drop=False,inplace=True)
    df.rename(columns={"level_0": "review_id","brewery_id": "brewery_id_src","clean_co": "brewery_id","beer_beerid": "beer_id"},inplace=True)
    

    
    #build our seperate datasets
    beers = df[['beer_id','beer_name','beer_style','beer_abv']].set_index('beer_id')
    reviews = df[['review_id','review_profilename','review_time','review_overall','review_aroma','review_appearance','review_palate','review_taste','beer_id','brewery_id']].set_index('review_id')
    print('Drop Duplicates on beers')
    beers.drop_duplicates(inplace=True)
    
    return beers,reviews