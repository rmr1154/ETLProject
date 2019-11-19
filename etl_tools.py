#pip install cleanco
#pip install fastparquet
from cleanco import cleanco
import pandas as pd
import sqlite3 as sq

def clean_co_names(df, col):
    df['clean_co'] = df[col]
    df['clean_co'] = df['clean_co'].str.upper() # uppercase
    print(f'>Set Upper')
    df['clean_co'] = df['clean_co'].str.replace(',', '') # Remove commas
    print(f'>Remove commas')
    df['clean_co'] = df['clean_co'].str.replace(' - ', ' ') # Remove hyphens
    print(f'>Remove hyphens')
    df['clean_co'] = df['clean_co'].str.replace(r"\(.*\)","") # Remove text between parenthesis 
    print(f'>Remove text between parens')
    df['clean_co'] = df['clean_co'].str.replace(' AND ', ' & ') #replace AND with &
    print(f'>replace AND with &')
    df['clean_co'] = df['clean_co'].str.strip() # Remove spaces in the begining/end
    print(f'>Remove leading/trailing spaces')
    df['clean_co'] = df['clean_co'] .apply(lambda x: cleanco(x).clean_name() if type(x)==str else x) # Remove business entities extensions (1)
    print(f'>Cleanco Pass1')
    df['clean_co'] = df['clean_co'].str.replace('.','') # Remove dots
    print(f'>Remove dots')
    df['clean_co'] = df['clean_co'] .str.encode('utf-8') # Encode
    print(f'>Encode utf-8')
    df['clean_co'] = df['clean_co'] .apply(lambda x: cleanco(x).clean_name() if type(x)==str else x) # Remove business entities extensions (2) - after removing the dots
    print(f'>Cleanco Pass2')
    return df

def strip_whitespace(df,col_list):
    for col in col_list:
        df[col] = df[col].str.strip()
    return df

def validate_url(df,col_list):
    for col in col_list:
        df[col] = validators.url

def web_screenshot(df,col):
    #grab screenshot w/ splinter
    return df

#Let's export everything to parquet
def export_parquet(table):
    for table in table_list:
        table.reset_index(drop=True).to_parquet('data/'+table+'.parquet', engine='fastparquet', compression='gzip') 
           
def hash_dim(table):
    
    return table